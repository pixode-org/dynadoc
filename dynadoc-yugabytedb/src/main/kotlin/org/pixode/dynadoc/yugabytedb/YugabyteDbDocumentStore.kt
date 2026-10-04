package org.pixode.dynadoc.yugabytedb

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.IsolationLevel
import io.r2dbc.spi.R2dbcException
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.withContext
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.UpdateConflictException

private const val LOCK_NOT_AVAILABLE = "55P03"
private const val SERIALIZATION_FAILURE = "40001"
private const val DEADLOCK_DETECTED = "40P01"
private const val UNIQUE_VIOLATION = "23505"

private const val HASH_COUNT = 65536

private val conflictStates: Set<String> =
    setOf(LOCK_NOT_AVAILABLE, SERIALIZATION_FAILURE, DEADLOCK_DETECTED, UNIQUE_VIOLATION)

/**
 * Represents an implementation of the [DocumentStore] interface that relies on the YSQL API of YugabyteDB for
 * persistence.
 *
 * Documents are stored in a range-sharded table whose primary key is made of a hash of the partition key, the
 * partition key and the local key, with a `version` column and a JSONB `body` column (see [createTable]). The hash
 * spreads the partitions evenly across the tablets, while the documents of a partition are stored together, sorted by
 * local key, and can be split across several tablets. Deleted documents are kept with a null body so that their
 * version is preserved.
 *
 * Updating multiple documents atomically relies on a Read Committed transaction, in which the rows of the documents
 * are locked without waiting before being checked and written. An update fails without waiting if any of its
 * documents is locked by a concurrent update.
 */
class YugabyteDbDocumentStore(
    private val connectionFactory: ConnectionFactory,
    tableName: String,
) : DocumentStore {

    private val rowMapper: RowMapper = RowMapper()
    private val table: String = tableName.split('.').joinToString(".", transform = ::quoteIdentifier)
    private val partitionFilter: String = "$PARTITION_HASH = ${hash("$1")} AND $PARTITION_KEY = $1"
    private val keyFilter: String = "$partitionFilter AND $LOCAL_KEY = $2"

    private val selectSql: String = "SELECT $PARTITION_KEY, $LOCAL_KEY, $VERSION, $BODY::text AS $BODY FROM $table"
    private val lockSql: String = "SELECT $PARTITION_KEY, $LOCAL_KEY, $VERSION FROM $table"
    private val insertSql: String =
        "INSERT INTO $table ($PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY, $VERSION, $BODY) " +
            "VALUES (${hash("$1")}, $1, $2, $3, CAST($4 AS jsonb)) ON CONFLICT DO NOTHING"
    private val updateSql: String =
        "UPDATE $table SET $VERSION = $3, $BODY = CAST($4 AS jsonb) WHERE $keyFilter AND $VERSION = $5"
    private val deleteSql: String = "DELETE FROM $table WHERE $keyFilter AND $VERSION = $3"

    // Locking the row in a subquery lets a single statement fail instead of waiting when the row is locked
    private val updateNoWaitSql: String =
        "$updateSql AND ($PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY) IN (" +
            "SELECT $PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY FROM $table " +
            "WHERE $keyFilter AND $VERSION = $5 FOR UPDATE NOWAIT)"

    //region updateDocuments

    override suspend fun updateDocuments(updatedDocuments: Iterable<Document>, checkedDocuments: Iterable<Document>) {
        val updatedList: List<Document> = updatedDocuments.toList()
        val checkedList: List<Document> = checkedDocuments.toList()

        when {
            updatedList.isEmpty() && checkedList.isEmpty() -> {}
            updatedList.size == 1 && checkedList.isEmpty() -> updateSingleDocument(updatedList[0])
            else -> updateMultipleDocuments(updatedList, checkedList)
        }
    }

    private suspend fun updateSingleDocument(document: Document) {
        val body: String? = rowMapper.fromBody(document)

        val rowsUpdated: Long = detectConflict({ document.id }) {
            withConnection { connection ->
                val sql: String = if (document.version == 0L) insertSql else updateNoWaitSql

                connection.createStatement(sql)
                    .bindWrite(document, body)
                    .rowsUpdated()
                    .single()
            }
        }

        if (rowsUpdated == 0L) {
            throw UpdateConflictException(document.id)
        }
    }

    private suspend fun updateMultipleDocuments(updatedDocuments: List<Document>, checkedDocuments: List<Document>) {
        // Serialize first, so that an invalid document fails before anything is written
        val bodies: Map<DocumentKey, String?> = updatedDocuments.associate { it.id to rowMapper.fromBody(it) }
        val updatedVersions: Map<DocumentKey, Long> = updatedDocuments.associate { it.id to it.version }

        // A document that is both updated and checked is only processed once
        val checkedOnly: List<Document> = checkedDocuments.filter { document ->
            val updatedVersion: Long? = updatedVersions[document.id]

            if (updatedVersion != null && updatedVersion != document.version) {
                throw UpdateConflictException(document.id)
            }

            updatedVersion == null
        }

        val documents: List<Document> = updatedDocuments + checkedOnly
        val ids: List<DocumentKey> = documents.map { it.id }.distinct()

        detectConflict({ findLockedDocument(ids) ?: ids[0] }) {
            withTransaction { connection ->
                // Lock the existing documents, so that a concurrent write to any of them causes a conflict
                val versions: Map<DocumentKey, Long> = lockDocuments(connection, ids)

                val outdated: Document? = documents.firstOrNull { (versions[it.id] ?: 0L) != it.version }
                if (outdated != null) {
                    throw UpdateConflictException(outdated.id)
                }

                val (inserted: List<Document>, updated: List<Document>) = updatedDocuments.partition { it.version == 0L }
                write(connection, insertSql, inserted) { bindWrite(it, bodies[it.id]) }
                write(connection, updateSql, updated) { bindWrite(it, bodies[it.id]) }

                // Insert then delete a placeholder, so that a concurrent insert of the same document causes a conflict
                val missing: List<Document> = checkedOnly.filter { it.version == 0L }
                write(connection, insertSql, missing) { bindPlaceholder(it.id) }
                write(connection, deleteSql, missing) { bindKey(it.id).bind(2, 0L) }
            }
        }
    }

    /**
     * Locks the rows of the documents that exist, failing if any of them is locked by another transaction, and returns
     * their versions.
     */
    private suspend fun lockDocuments(connection: Connection, ids: List<DocumentKey>): Map<DocumentKey, Long> =
        connection.createStatement("$lockSql WHERE ${keysFilter(ids.size)} FOR UPDATE NOWAIT")
            .bindKeys(ids)
            .rows { row -> rowMapper.toDocumentKey(row) to rowMapper.toVersion(row) }
            .toList()
            .toMap()

    /**
     * Executes the statement once for each document, failing if any of the documents is not written.
     */
    private suspend fun write(
        connection: Connection,
        sql: String,
        documents: List<Document>,
        bind: Statement.(Document) -> Statement,
    ) {
        if (documents.isEmpty()) {
            return
        }

        val statement: Statement = connection.createStatement(sql)
        documents.forEachIndexed { index, document ->
            if (index > 0) {
                statement.add()
            }
            statement.bind(document)
        }

        val notWritten: Int = statement.rowsUpdated().toList().indexOf(0L)
        if (notWritten >= 0) {
            throw UpdateConflictException(documents[notWritten].id)
        }
    }

    /**
     * Finds the first document that is locked by another transaction.
     */
    private suspend fun findLockedDocument(ids: List<DocumentKey>): DocumentKey? =
        ids.firstOrNull { id ->
            try {
                withTransaction { connection -> lockDocuments(connection, listOf(id)) }
                false
            } catch (exception: R2dbcException) {
                if (isConflict(exception)) true else throw exception
            }
        }

    private inline fun <T> detectConflict(id: () -> DocumentKey, action: () -> T): T =
        try {
            action()
        } catch (exception: R2dbcException) {
            if (isConflict(exception)) {
                throw UpdateConflictException(id())
            } else {
                throw exception
            }
        }

    private fun isConflict(exception: R2dbcException): Boolean =
        exception.sqlState.orEmpty() in conflictStates

    private fun Statement.bindWrite(document: Document, body: String?): Statement {
        bindKey(document.id).bind(2, document.version + 1)

        if (body == null) {
            bindNull(3, String::class.java)
        } else {
            bind(3, body)
        }

        return if (document.version == 0L) this else bind(4, document.version)
    }

    private fun Statement.bindPlaceholder(id: DocumentKey): Statement =
        bindKey(id).bind(2, 0L).bindNull(3, String::class.java)

    //endregion

    //region getDocuments

    override fun getDocuments(ids: Iterable<DocumentKey>): Flow<Document> {
        val idList: List<DocumentKey> = ids.toList()

        return if (idList.isEmpty()) {
            emptyFlow()
        } else {
            getMultipleDocuments(idList)
        }
    }

    private fun getMultipleDocuments(idList: List<DocumentKey>): Flow<Document> = flow {
        val distinctIds: List<DocumentKey> = idList.distinct()

        val documents: Map<DocumentKey, Document> = withConnection { connection ->
            connection.createStatement("$selectSql WHERE ${keysFilter(distinctIds.size)}")
                .bindKeys(distinctIds)
                .rows(rowMapper::toDocument)
                .toList()
                .associateBy { it.id }
        }

        for (id in idList) {
            emit(documents[id] ?: Document(id, null, 0))
        }
    }

    //endregion

    /**
     * Retrieves the documents of a partition, sorted by local key, whose local key is greater than or equal to
     * [startLocalKey] and lower than [endLocalKey]. When [endLocalKey] is null, all the documents of the partition
     * starting from [startLocalKey] are returned. Deleted documents are included, with a null body.
     */
    fun scan(partitionKey: String, startLocalKey: String = "", endLocalKey: String? = null): Flow<Document> {
        val range: String = if (endLocalKey == null) "" else " AND $LOCAL_KEY < $3"

        return select("$partitionFilter AND $LOCAL_KEY >= $2$range ORDER BY $LOCAL_KEY") {
            bind(0, partitionKey).bind(1, startLocalKey)

            if (endLocalKey != null) {
                bind(2, endLocalKey)
            }
        }
    }

    /**
     * Finds the documents matching the given condition, which is a SQL expression following the `WHERE` keyword and
     * referring to the columns `partition_hash`, `partition_key`, `local_key`, `version` and `body`. The values of the
     * parameters `$1`, `$2`, etc. are given by [values], in that order.
     *
     * The documents of a partition are read directly when the condition specifies both the hash of the partition key
     * and the partition key, as in `partition_hash = yb_hash_code($1::text COLLATE "C") AND partition_key = $1`.
     */
    fun query(where: String, vararg values: Any, configure: Statement.() -> Unit = { }): Flow<Document> =
        select(where) {
            values.forEachIndexed { index, value -> bind(index, value) }
            configure()
        }

    private fun select(where: String, configure: Statement.() -> Unit): Flow<Document> = flow {
        withConnection { connection ->
            val documents: Flow<Document> = connection.createStatement("$selectSql WHERE $where")
                .apply(configure)
                .rows(rowMapper::toDocument)

            emitAll(documents)
        }
    }

    /**
     * Creates the table, range-sharded by hash of the partition key, then partition key, then local key, so that the
     * partitions are spread evenly across the tablets, and the documents of a partition are stored together and
     * sorted by local key.
     *
     * The table is initially split into the given number of [tablets], each holding an equal share of the hashes.
     * Tablets are then split automatically as they grow, including within a partition.
     */
    suspend fun createTable(tablets: Int = 1) {
        require(tablets in 1..HASH_COUNT) {
            "The number of tablets must be between 1 and $HASH_COUNT"
        }

        val split: String =
            if (tablets == 1) {
                ""
            } else {
                " SPLIT AT VALUES (${(1 until tablets).joinToString { i -> "(${HASH_COUNT * i / tablets})" }})"
            }

        withConnection { connection ->
            connection
                .createStatement(
                    """
                    CREATE TABLE IF NOT EXISTS $table (
                        $PARTITION_HASH INT NOT NULL,
                        $PARTITION_KEY TEXT COLLATE "C" NOT NULL,
                        $LOCAL_KEY TEXT COLLATE "C" NOT NULL,
                        $VERSION BIGINT NOT NULL,
                        $BODY JSONB,
                        PRIMARY KEY ($PARTITION_HASH ASC, $PARTITION_KEY ASC, $LOCAL_KEY ASC),
                        CHECK ($PARTITION_HASH = yb_hash_code($PARTITION_KEY))
                    )$split
                    """.trimIndent()
                )
                .rowsUpdated()
                .toList()
        }
    }

    private suspend fun <T> withConnection(action: suspend (Connection) -> T): T {
        val connection: Connection = connectionFactory.create().awaitSingle()

        try {
            return action(connection)
        } finally {
            withContext(NonCancellable) {
                connection.close().awaitFirstOrNull()
            }
        }
    }

    private suspend fun <T> withTransaction(action: suspend (Connection) -> T): T =
        withConnection { connection ->
            connection.beginTransaction(IsolationLevel.READ_COMMITTED).awaitFirstOrNull()

            try {
                val result: T = action(connection)
                connection.commitTransaction().awaitFirstOrNull()
                result
            } catch (exception: Throwable) {
                withContext(NonCancellable) {
                    runCatching { connection.rollbackTransaction().awaitFirstOrNull() }
                }
                throw exception
            }
        }

    private fun Statement.rowsUpdated(): Flow<Long> =
        execute().asFlow().map { result -> result.rowsUpdated.awaitFirstOrNull() ?: 0L }

    private fun <T : Any> Statement.rows(transform: (Row) -> T): Flow<T> = flow {
        execute().asFlow().collect { result ->
            emitAll(result.map { row, _ -> transform(row) }.asFlow())
        }
    }

    private fun Statement.bindKey(id: DocumentKey): Statement =
        bind(0, id.partitionKey).bind(1, id.localKey)

    private fun Statement.bindKeys(ids: List<DocumentKey>): Statement {
        ids.forEachIndexed { index, id ->
            bind(index * 2, id.partitionKey).bind(index * 2 + 1, id.localKey)
        }

        return this
    }

    // An equality on the whole primary key lets each row be read directly
    private fun keysFilter(count: Int): String =
        if (count == 1) {
            keyFilter
        } else {
            val keys: String = (0 until count).joinToString { i ->
                val partitionKey = "$${i * 2 + 1}"
                "(${hash(partitionKey)}, $partitionKey, $${i * 2 + 2})"
            }
            "($PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY) IN ($keys)"
        }

    // The hash used by YugabyteDB for hash sharding, which is evenly distributed between 0 and HASH_COUNT - 1
    private fun hash(partitionKey: String): String = "yb_hash_code($partitionKey::text COLLATE \"C\")"

    private fun quoteIdentifier(identifier: String): String = "\"${identifier.replace("\"", "\"\"")}\""
}
