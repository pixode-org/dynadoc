package org.pixode.dynadoc.tidb

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.R2dbcException
import io.r2dbc.spi.Statement
import java.time.Clock
import java.time.Duration
import java.util.Optional
import kotlin.jvm.optionals.getOrNull
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.last
import kotlinx.coroutines.flow.lastOrNull
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
 * key and the local key, with a `version` column and a JSON `body` column (see [createTable]). The hash spreads the
 * partitions evenly across the regions, while the documents of a partition are stored together, sorted by local key,
 * and can be split across several regions. Deleted documents are kept with a null body so that their version is
 * preserved, until they are removed by the TTL jobs of the database, once [expiration] has elapsed.
 *
 * A single document is updated by a single statement. TiDB doesn't support stored procedures, so multiple documents
 * are updated by a script whose statements are sent together, with the documents as its parameters. An update is
 * therefore a single round trip to the database, and a single transaction. The script first inserts the documents
 * into a temporary table (see [createTable]), whose rows are kept in memory and are private to the transaction. It
 * then checks and writes the documents.
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

    private val partitionFilter: String = "$PARTITION_HASH = CRC32(?) AND $PARTITION_KEY = ?"
    private val keyFilter: String = "$partitionFilter AND $LOCAL_KEY = ?"

    private val selectSql: String =
        "SELECT $PARTITION_KEY, $LOCAL_KEY, $VERSION, CAST($BODY AS CHAR) AS $BODY FROM ${table.tableName}"

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
                                .bind(0, document.id.partitionKey)
                                .bind(1, document.id.partitionKey)
                                .bind(2, document.id.localKey)
                                .bindNullable(3, body, String::class.java)
                                .bindNullable(4, deleted, Long::class.javaObjectType)
                        } else {
                            connection.createStatement(table.updateSql)
                                .bindNullable(0, body, String::class.java)
                                .bindNullable(1, deleted, Long::class.javaObjectType)
                                .bind(2, document.id.partitionKey)
                                .bind(3, document.id.partitionKey)
                                .bind(4, document.id.localKey)
                                .bind(5, document.version)
                        }

                    statement.rowsUpdated().toList().sum()
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

    private suspend fun updateMultipleDocuments(updatedList: List<Document>, checkedList: List<Document>) {
        val documents: List<Document> = updatedList + checkedList

        val conflict: Long? = withConnection { connection ->
            try {
                // Each statement of the script has its own result, the last one being the result of the script
                connection.createStatement(table.updateScript(documents.size))
                    .apply { bindDocuments(updatedList, checkedList) }
                    .execute()
                    .asFlow()
                    .map { result ->
                        result.map { row -> Optional.ofNullable(row.get(0, Long::class.javaObjectType)) }
                            .asFlow()
                            .lastOrNull()
                    }
                    .last()
                    ?.getOrNull()
            } catch (exception: Throwable) {
                // The script stops at the statement that failed, which can leave its transaction open
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

    private fun Statement.bindDocuments(updated: List<Document>, checked: List<Document>) {
        var index = 0

        // The time from which the documents deleted by the update can be removed
        bind(index++, expirationTime())

        fun bindDocument(document: Document, body: JsonElement?, check: Boolean) {
            bind(index++, document.id.partitionKey)
            bind(index++, document.id.partitionKey)
            bind(index++, document.id.localKey)
            bind(index++, document.version)
            bindNullable(index++, fromBody(body), String::class.java)
            bind(index++, check)
        }

        updated.forEach { document -> bindDocument(document, document.body, false) }
        checked.forEach { document -> bindDocument(document, null, true) }
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
            connection.createStatement(selectKeysSql(distinctIds.size))
                .apply { bindKeys(distinctIds) }
                .asDocuments()
                .toList()
                .associateBy { it.id }
        }

        for (id in idList) {
            emit(documents[id] ?: Document(id, null, 0))
        }
    }

    // Each row is read directly using an equality on the whole primary key, whereas a single condition matching all
    // the keys can result in the whole table being read
    private fun selectKeysSql(count: Int): String =
        (0 until count).joinToString(" UNION ALL ") { "$selectSql WHERE $keyFilter" }

    private fun Statement.bindKeys(ids: List<DocumentKey>) {
        ids.forEachIndexed { index, id ->
            bind(index * 3, id.partitionKey)
            bind(index * 3 + 1, id.partitionKey)
            bind(index * 3 + 2, id.localKey)
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
                .asDocuments()

            emitAll(documents)
        }
    }

    //endregion

    //region createTable

    /**
     * Creates the table, clustered by hash of the partition key, then partition key, then local key, so that the
     * partitions are spread evenly across the regions, and the documents of a partition are stored together and
     * sorted by local key.
     *
     * The temporary table holding the documents of an update is created along with the table. It has the name of the
     * table followed by `_update`.
     */
    suspend fun createTable() {
        withConnection { connection ->
            connection.createStatement(table.createTableSql).rowsUpdated().toList()
            connection.createStatement(table.createUpdateTableSql).rowsUpdated().toList()
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

    private fun Statement.asDocuments(): Flow<Document> = flow {
        execute().asFlow().collect { result ->
            emitAll(result.map(RowMapper::toDocument).asFlow())
        }
    }
}
