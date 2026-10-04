package org.pixode.dynadoc.tidb

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.R2dbcException
import io.r2dbc.spi.Result
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import java.util.Optional
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.withContext
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.UpdateConflictException

private const val DUPLICATE_ENTRY = 1062
private const val LOCK_WAIT_TIMEOUT = 1205
private const val DEADLOCK = 1213
private const val LOCK_NOT_AVAILABLE = 3572
private const val WRITE_CONFLICT = 9007

private val conflictCodes: Set<Int> =
    setOf(DUPLICATE_ENTRY, LOCK_WAIT_TIMEOUT, DEADLOCK, LOCK_NOT_AVAILABLE, WRITE_CONFLICT)

/**
 * Represents an implementation of the [DocumentStore] interface that relies on TiDB for persistence.
 *
 * Documents are stored in a table whose clustered primary key is made of a hash of the partition key, the partition
 * key and the local key, with a `version` column and a JSON `body` column (see [createTable]). The hash spreads the
 * partitions evenly across the regions, while the documents of a partition are stored together, sorted by local key,
 * and can be split across several regions. Deleted documents are kept with a null body so that their version is
 * preserved.
 *
 * TiDB doesn't support stored procedures, so documents are updated by a script whose statements are sent together,
 * with the documents as a single JSON parameter. An update is therefore a single round trip to the database, and a
 * single transaction. The script locks the rows of the documents without waiting before checking and writing them, so
 * an update fails without waiting if any of its documents is locked by a concurrent update.
 */
class TiDbDocumentStore(
    private val connectionFactory: ConnectionFactory,
    private val tableName: String,
) : DocumentStore {
    private val table = TiDbTable(tableName)

    private val partitionFilter: String = "$PARTITION_HASH = CRC32(?) AND $PARTITION_KEY = ?"
    private val keyFilter: String = "$partitionFilter AND $LOCAL_KEY = ?"

    private val selectSql: String =
        "SELECT $PARTITION_KEY, $LOCAL_KEY, $VERSION, CAST($BODY AS CHAR) AS $BODY FROM ${table.tableName}"
    private val updateScript: String = table.updateScript()

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

        require(documents.size <= MAX_UPDATE_SIZE) {
            "No more than $MAX_UPDATE_SIZE documents can be updated at once"
        }

        val operations: String = RowMapper.fromDocuments(updatedList, checkedList)

        val conflict: Long? = withConnection { connection ->
            try {
                // Each statement of the script has its own result, the last one being the result of the script
                connection.createStatement(updateScript)
                    .bind(0, operations)
                    .execute()
                    .asFlow()
                    .map { result ->
                        result.rows { row -> Optional.ofNullable(row.get(0, Long::class.javaObjectType)) }.toList()
                    }
                    .toList()
                    .last()
                    .single()
                    .orElse(null)
            } catch (exception: Throwable) {
                // The script stops at the statement that failed, leaving its transaction open
                withContext(NonCancellable) {
                    connection.createStatement("ROLLBACK").rowsUpdated().toList()
                }

                // The conflict has been detected by the database rather than by the script
                if (exception is R2dbcException && exception.errorCode in conflictCodes) {
                    throw UpdateConflictException(documents[0].id)
                } else {
                    throw exception
                }
            }
        }

        if (conflict != null) {
            throw UpdateConflictException(documents[conflict.toInt()].id)
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
     * Retrieves the documents of a partition, sorted by local key, whose local key is greater than or equal to
     * [startLocalKey] and lower than [endLocalKey]. When [endLocalKey] is null, all the documents of the partition
     * starting from [startLocalKey] are returned. Deleted documents are included, with a null body.
     */
    fun scan(partitionKey: String, startLocalKey: String = "", endLocalKey: String? = null): Flow<Document> {
        val range: String = if (endLocalKey == null) "" else " AND $LOCAL_KEY < ?"

        return select("$partitionFilter AND $LOCAL_KEY >= ?$range ORDER BY $LOCAL_KEY") {
            bind(0, partitionKey).bind(1, partitionKey).bind(2, startLocalKey)

            if (endLocalKey != null) {
                bind(3, endLocalKey)
            }
        }
    }

    /**
     * Finds the documents matching the given condition, which is a SQL expression following the `WHERE` keyword and
     * referring to the columns `partition_hash`, `partition_key`, `local_key`, `version` and `body`. The values of the
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
                .rows(RowMapper::toDocument)

            emitAll(documents)
        }
    }

    //endregion

    //region createTable

    /**
     * Creates the table, clustered by hash of the partition key, then partition key, then local key, so that the
     * partitions are spread evenly across the regions, and the documents of a partition are stored together and
     * sorted by local key.
     */
    suspend fun createTable() {
        withConnection { connection ->
            connection.createStatement(table.createTableSql()).rowsUpdated().toList()
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
            emitAll(result.rows(transform))
        }
    }

    private fun <T : Any> Result.rows(transform: (Row) -> T): Flow<T> =
        map { row, _ -> transform(row) }.asFlow()

    private fun Statement.bindKeys(ids: List<DocumentKey>): Statement {
        ids.forEachIndexed { index, id ->
            bind(index * 3, id.partitionKey).bind(index * 3 + 1, id.partitionKey).bind(index * 3 + 2, id.localKey)
        }

        return this
    }

    // Each row is read directly using an equality on the whole primary key, whereas a single condition matching all
    // the keys can result in the whole table being read
    private fun selectKeysSql(count: Int): String =
        (0 until count).joinToString(" UNION ALL ") { "$selectSql WHERE $keyFilter" }
}
