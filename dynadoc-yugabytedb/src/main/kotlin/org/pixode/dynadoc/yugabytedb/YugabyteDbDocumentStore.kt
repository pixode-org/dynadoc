package org.pixode.dynadoc.yugabytedb

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
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

private val conflictStates: Set<String> =
    setOf(LOCK_NOT_AVAILABLE, SERIALIZATION_FAILURE, DEADLOCK_DETECTED, UNIQUE_VIOLATION)

/**
 * Represents an implementation of the [DocumentStore] interface that relies on the YSQL API of YugabyteDB for
 * persistence.
 *
 * Documents are stored in a range-sharded table whose primary key is made of a hash of the partition key, the
 * partition key and the sort key, with a `version` column and a JSONB `body` column (see [createTable]). The hash
 * spreads the partitions evenly across the tablets, while the documents of a partition are stored together, sorted by
 * sort key, and can be split across several tablets. Deleted documents are kept with a null body so that their
 * version is preserved.
 *
 * Documents are updated by a function stored in the database (see [createTable]), so that an update is a single round
 * trip to the database, and a single transaction. The function locks the rows of the documents without waiting before
 * checking and writing them, so an update fails without waiting if any of its documents is locked by a concurrent
 * update.
 */
class YugabyteDbDocumentStore(
    private val connectionFactory: ConnectionFactory,
    private val tableName: String,
) : DocumentStore {
    private val table = YugabyteDbTable(tableName)

    private val partitionFilter: String = "$PARTITION_HASH = ${hash("$1")} AND $PARTITION_KEY = $1"

    private val selectSql: String = "SELECT $PARTITION_KEY, $SORT_KEY, $VERSION, $BODY::text AS $BODY FROM ${table.tableName}"
    private val updateSql: String = "SELECT ${table.updateFunction}(CAST($1 AS jsonb))"

    //region updateDocuments

    override suspend fun updateDocuments(updatedDocuments: Iterable<Document>, checkedDocuments: Iterable<Document>) {
        val updatedList: List<Document> = updatedDocuments.toList()
        val updatedVersions: Map<DocumentKey, Long> = updatedList.associate { it.id to it.version }

        // A document that is both updated and checked is only processed once
        val checkedList: List<Document> = checkedDocuments.filter { document ->
            val updatedVersion: Long? = updatedVersions[document.id]

            if (updatedVersion != null && updatedVersion != document.version) {
                throw UpdateConflictException(document.id)
            }

            updatedVersion == null
        }

        val documents: List<Document> = updatedList + checkedList
        if (documents.isEmpty()) {
            return
        }

        val operations: String = RowMapper.fromDocuments(updatedList, checkedList)

        val conflict: Int =
            try {
                withConnection { connection ->
                    connection.createStatement(updateSql)
                        .bind(0, operations)
                        .rows { row -> checkNotNull(row.get(0, Int::class.javaObjectType)) }
                        .single()
                }
            } catch (exception: R2dbcException) {
                // The conflict has been detected by the database rather than by the function
                if (exception.sqlState.orEmpty() in conflictStates) {
                    throw UpdateConflictException(documents[0].id)
                } else {
                    throw exception
                }
            }

        if (conflict >= 0) {
            throw UpdateConflictException(documents[conflict].id)
        }
    }

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
            connection.createStatement(selectKeysSql(distinctIds.size))
                .bindKeys(distinctIds)
                .rows(RowMapper::toDocument)
                .toList()
                .associateBy { it.id }
        }

        for (id in idList) {
            emit(documents[id] ?: Document(id, null, 0))
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
        val range: String = if (endSortKey == null) "" else " AND $SORT_KEY < $3"

        return select("$partitionFilter AND $SORT_KEY >= $2$range ORDER BY $SORT_KEY") {
            bind(0, partitionKey).bind(1, startSortKey)

            if (endSortKey != null) {
                bind(2, endSortKey)
            }
        }
    }

    /**
     * Finds the documents matching the given condition, which is a SQL expression following the `WHERE` keyword and
     * referring to the columns `partition_hash`, `partition_key`, `sort_key`, `version` and `body`. The values of the
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
                .rows(RowMapper::toDocument)

            emitAll(documents)
        }
    }

    //endregion

    //region createTable

    /**
     * Creates the table, range-sharded by hash of the partition key, then partition key, then sort key, so that the
     * partitions are spread evenly across the tablets, and the documents of a partition are stored together and
     * sorted by sort key.
     *
     * The function used to update the documents of the table is created along with the table, or replaced if it
     * already exists. It has the name of the table followed by `_update`.
     */
    suspend fun createTable() {
        withConnection { connection ->
            connection.createStatement(table.createTableSql()).rowsUpdated().toList()
            connection.createStatement(table.createFunctionSql()).rowsUpdated().toList()
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

    private fun Statement.rowsUpdated(): Flow<Long> =
        execute().asFlow().map { result -> result.rowsUpdated.awaitFirstOrNull() ?: 0L }

    private fun <T : Any> Statement.rows(transform: (Row) -> T): Flow<T> = flow {
        execute().asFlow().collect { result ->
            emitAll(result.map { row, _ -> transform(row) }.asFlow())
        }
    }

    private fun Statement.bindKeys(ids: List<DocumentKey>): Statement {
        ids.forEachIndexed { index, id ->
            bind(index * 2, id.partitionKey).bind(index * 2 + 1, id.sortKey)
        }

        return this
    }

    // Each row is read directly using an equality on the whole primary key, whereas a single condition matching all
    // the keys can result in the whole table being read
    private fun selectKeysSql(count: Int): String =
        (0 until count).joinToString(" UNION ALL ") { i ->
            val partitionKey = "$${i * 2 + 1}"
            val sortKey = "$${i * 2 + 2}"

            "$selectSql WHERE $PARTITION_HASH = ${hash(partitionKey)} " +
                "AND $PARTITION_KEY = $partitionKey AND $SORT_KEY = $sortKey"
        }

    // The hash used by YugabyteDB for hash sharding, which is evenly distributed between 0 and HASH_COUNT - 1
    private fun hash(partitionKey: String): String = "yb_hash_code($partitionKey::text COLLATE \"C\")"
}
