package org.pixode.dynadoc.tidb

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.R2dbcException
import io.r2dbc.spi.Statement
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.UpdateConflictException

private const val DUPLICATE_ENTRY = 1062
private const val LOCK_WAIT_TIMEOUT = 1205
private const val DEADLOCK = 1213
private const val WRITE_CONFLICT = 9007

private val conflictCodes: Set<Int> =
    setOf(DUPLICATE_ENTRY, LOCK_WAIT_TIMEOUT, DEADLOCK, WRITE_CONFLICT)

/**
 * Represents an implementation of the [DocumentStore] interface that relies on TiDB for persistence.
 *
 * Documents are stored in a table whose clustered primary key is made of a hash of the partition key, the partition
 * key and the sort key, with a `version` column and a JSON `body` column (see [createTable]). The hash spreads the
 * partitions evenly across the regions, while the documents of a partition are stored together, sorted by sort key,
 * and can be split across several regions. Deleted documents are kept with a null body so that their version is
 * preserved, until they are removed by the TTL jobs of the database, once [expiration] has elapsed.
 *
 * A single document is updated by a single statement. Multiple documents are updated by a transaction made of a fixed
 * number of statements, whatever the number of documents: the transaction begins, reads the versions of the documents,
 * writes them all with a single statement, then commits. Statements with the same shape are executed many times, so
 * that they can be prepared (see the README).
 *
 * Updates are optimistic transactions, which don't lock the documents: an update fails when it is committed if any
 * of its documents has been written by a concurrent transaction.
 */
class TiDbDocumentStore(
    private val connectionFactory: ConnectionFactory,
    private val tableName: String,
    private val expiration: Duration = Duration.ofDays(30),
    private val clock: Clock = Clock.systemUTC(),
) : DocumentStore {
    private val table = TiDbTable(tableName)

    private val partitionFilter: String = "$PARTITION_HASH = ? AND $PARTITION_KEY = ?"

    private val selectSql: String = table.selectSql

    //region updateDocuments

    override suspend fun updateDocuments(updatedDocuments: Iterable<Document>, checkedDocuments: Iterable<Document>) {
        val updatedList: List<Document> = updatedDocuments.toList()
        val checkedList: List<Document> = checkedDocuments.toList()
        val documents: List<Document> = updatedList + checkedList

        require(documents.distinctBy { it.id }.size == documents.size) {
            "A document can only be updated or checked once"
        }

        when {
            documents.isEmpty() -> {}
            updatedList.size == 1 && checkedList.isEmpty() -> updateSingleDocument(updatedList[0])
            else -> updateMultipleDocuments(updatedList, checkedList)
        }
    }

    private suspend fun updateSingleDocument(document: Document) {
        val body: String? = fromBody(document.body)
        // The time from which the document can be removed if it is deleted
        val deleted: Long? = if (body == null) expirationTime() else null

        val rowsUpdated: Long =
            try {
                withConnection { connection ->
                    val statement: Statement =
                        if (document.version == 0L) {
                            connection.createStatement(table.insertSql)
                                .bind(0, partitionHash(document.id.partitionKey))
                                .bind(1, document.id.partitionKey)
                                .bind(2, document.id.sortKey)
                                .bindNullable(3, body, String::class.java)
                                .bindNullable(4, deleted, Long::class.javaObjectType)
                        } else {
                            connection.createStatement(table.updateSql)
                                .bindNullable(0, body, String::class.java)
                                .bindNullable(1, deleted, Long::class.javaObjectType)
                                .bind(2, partitionHash(document.id.partitionKey))
                                .bind(3, document.id.partitionKey)
                                .bind(4, document.id.sortKey)
                                .bind(5, document.version)
                        }

                    statement.rowsUpdated()
                }
            } catch (exception: R2dbcException) {
                // The document has been created or written by a concurrent transaction
                if (exception.errorCode in conflictCodes) {
                    throw UpdateConflictException(document.id)
                } else {
                    throw exception
                }
            }

        if (rowsUpdated == 0L) {
            throw UpdateConflictException(document.id)
        }
    }

    private suspend fun updateMultipleDocuments(updatedDocuments: List<Document>, checkedDocuments: List<Document>) {
        val documents: Sequence<Document> = updatedDocuments.asSequence() + checkedDocuments.asSequence()

        withConnection { connection ->
            try {
                connection.createStatement("BEGIN OPTIMISTIC").rowsUpdated()

                // Reading the documents for update makes the commit fail if a concurrent transaction writes any
                // of them, including the documents that don't exist, because each document is read by its primary key.
                val versions: Map<DocumentKey, Long> =
                    connection.createStatement(table.selectVersionsSql(updatedDocuments.size + checkedDocuments.size))
                        .apply { bindKeys(documents.map { it.id }) }
                        .readVersions()

                documents.firstOrNull { (versions[it.id] ?: 0) != it.version }?.let { conflict ->
                    throw UpdateConflictException(conflict.id)
                }

                if (updatedDocuments.isNotEmpty()) {
                    connection.createStatement(table.upsertSql(updatedDocuments.size))
                        .apply { bindUpsertDocuments(updatedDocuments) }
                        .rowsUpdated()
                }

                connection.createStatement("COMMIT").rowsUpdated()
            } catch (exception: Throwable) {
                // The transaction is still open if the failure didn't happen when it was committed
                withContext(NonCancellable) {
                    connection.createStatement("ROLLBACK").rowsUpdated()
                }

                // The conflict has been detected by the database rather than by the store
                if (exception is R2dbcException && exception.errorCode in conflictCodes) {
                    throw UpdateConflictException(documents.first().id)
                } else {
                    throw exception
                }
            }
        }
    }

    private suspend fun Statement.readVersions(): Map<DocumentKey, Long> = execute()
        .awaitSingle()
        .map { row -> RowMapper.toDocumentKey(row) to RowMapper.toVersion(row) }
        .asFlow()
        .fold(HashMap()) { versions, (id, version) -> versions.also { it[id] = version } }

    private fun Statement.bindUpsertDocuments(documents: List<Document>) {
        // The time from which the documents can be removed if they are deleted
        val deleted: Long = expirationTime()
        var index = 0

        for ((id, body, version) in documents) {
            bind(index++, partitionHash(id.partitionKey))
            bind(index++, id.partitionKey)
            bind(index++, id.sortKey)
            bind(index++, version)
            bindNullable(index++, fromBody(body), String::class.java)
            bindNullable(index++, if (body == null) deleted else null, Long::class.javaObjectType)
        }
    }

    private fun fromBody(body: JsonElement?): String? {
        require(body == null || body is JsonObject) { "The document must be a valid JSON object" }
        return body?.toString()
    }

    private fun expirationTime(): Long = (clock.instant() + expiration).epochSecond

    private fun <T : Any> Statement.bindNullable(index: Int, value: T?, type: Class<T>): Statement =
        if (value == null) bindNull(index, type) else bind(index, value)

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
            connection.createStatement(table.selectDocumentsSql(distinctIds.size))
                .apply { bindKeys(distinctIds.asSequence()) }
                .asDocuments()
                .toList()
                .associateBy { it.id }
        }

        for (id in idList) {
            emit(documents[id] ?: Document(id, null, 0))
        }
    }

    private fun Statement.bindKeys(ids: Sequence<DocumentKey>) {
        var index = 0

        for ((partitionKey, sortKey) in ids) {
            bind(index++, partitionHash(partitionKey))
            bind(index++, partitionKey)
            bind(index++, sortKey)
        }
    }

    //endregion

    //region scan and query

    /**
     * Retrieves the documents of a partition, sorted by sort key, whose sort key is greater than or equal to
     * [startSortKey] and lower than [endSortKey]. When [endSortKey] is null, all the documents of the partition
     * starting from [startSortKey] are returned. Deleted documents are included, with a null body.
     */
    fun scan(partitionKey: String, startSortKey: String = "", endSortKey: String? = null): Flow<Document> {
        val range: String = if (endSortKey == null) "" else " AND $SORT_KEY < ?"

        return select("$partitionFilter AND $SORT_KEY >= ?$range ORDER BY $SORT_KEY") {
            bind(0, partitionHash(partitionKey)).bind(1, partitionKey).bind(2, startSortKey)

            if (endSortKey != null) {
                bind(3, endSortKey)
            }
        }
    }

    /**
     * Finds the documents matching the given condition, which is a SQL expression following the `WHERE` keyword and
     * referring to the columns `partition_hash`, `partition_key`, `sort_key`, `version` and `body`. The values of the
     * parameters `?` are given by [values], in that order.
     *
     * The documents of a partition are read directly when the condition specifies both the hash of the partition key
     * and the partition key, as in `partition_hash = CRC32(?) AND partition_key = ?`.
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
                .asDocuments()

            emitAll(documents)
        }
    }

    //endregion

    //region createTable

    /**
     * Creates the table, clustered by hash of the partition key, then partition key, then sort key, so that the
     * partitions are spread evenly across the regions, and the documents of a partition are stored together and
     * sorted by sort key.
     */
    suspend fun createTable() {
        withConnection { connection ->
            connection.createStatement(table.createTableSql).rowsUpdated()
        }
    }

    //endregion

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

    private suspend fun Statement.rowsUpdated(): Long = execute()
        .asFlow()
        .map { result -> result.rowsUpdated.awaitFirstOrNull() ?: 0L }
        .fold(0L) { sum, count -> sum + count }

    private fun Statement.asDocuments(): Flow<Document> = flow {
        execute().asFlow().collect { result ->
            emitAll(result.map(RowMapper::toDocument).asFlow())
        }
    }
}
