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
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.SortDirection
import org.pixode.dynadoc.core.SortKeyBound
import org.pixode.dynadoc.core.SortKeyRange
import org.pixode.dynadoc.core.UpdateConflictException

/**
 * Represents an implementation of the [DocumentStore] interface that relies on TiDB for persistence.
 */
class TiDbDocumentStore(
    private val connectionFactory: ConnectionFactory,
    tableName: String,
    private val expiration: Duration = Duration.ofDays(30),
    private val clock: Clock = Clock.systemUTC(),
) : DocumentStore {
    private val table = TiDbTable(tableName)

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

        val rowsUpdated: Long = try {
            withConnection { connection ->
                if (document.version == 0L) {
                    try {
                        connection.insertSingleDocument(document).rowsUpdated()
                    } catch (exception: R2dbcException) {
                        if (exception.isPrimaryKeyViolation()) {
                            connection.updateSingleDocument(document, body, deleted).rowsUpdated()
                        } else {
                            throw exception
                        }
                    }
                } else {
                    connection.updateSingleDocument(document, body, deleted).rowsUpdated()
                }
            }
        } catch (exception: R2dbcException) {
            // The document has been created or updated by a concurrent transaction
            if (exception.isConflict()) {
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

                // Selecting for update makes the commit fail if a concurrent transaction modifies any of the documents
                val versions: Map<DocumentKey, Long> = connection.readVersions(documents.map { it.id }.toList())

                documents.firstOrNull { (versions[it.id] ?: 0) != it.version }?.let { conflict ->
                    throw UpdateConflictException(conflict.id)
                }

                // A checked document that doesn't exist can't be read for update, so it is written as a placeholder
                // with a version of 0, which makes the commit fail if a concurrent transaction creates it
                val writtenDocuments: List<Document> =
                    updatedDocuments.map { it.copy(version = it.version + 1) } +
                    checkedDocuments.filter { it.id !in versions }.map { Document(it.id, null, 0) }

                val (existing, new) = writtenDocuments.partition { it.id in versions }
                if (new.isNotEmpty()) {
                    connection.insertMultipleDocuments(new).rowsUpdated()
                }
                if (existing.isNotEmpty()) {
                    connection.updateMultipleDocuments(existing).rowsUpdated()
                }

                connection.createStatement("COMMIT").rowsUpdated()
            } catch (exception: Throwable) {
                // The transaction is still open if the failure didn't happen when it was committed
                withContext(NonCancellable) {
                    connection.createStatement("ROLLBACK").rowsUpdated()
                }

                // The conflict has been detected by the database rather than by the store
                if (exception is R2dbcException && exception.isConflict()) {
                    throw UpdateConflictException(documents.first().id)
                } else {
                    throw exception
                }
            }
        }
    }

    private suspend fun Connection.readVersions(ids: List<DocumentKey>): Map<DocumentKey, Long> =
        createStatement(table.selectVersionsSql(ids.size))
            .bindKeys(ids)
            .execute()
            .awaitSingle()
            .map { row -> RowMapper.toDocumentKey(row) to RowMapper.toVersion(row) }
            .asFlow()
            .fold(mutableMapOf()) { versions, (id, version) -> versions.also { it[id] = version } }

    private fun Connection.insertSingleDocument(document: Document): Statement =
        insertMultipleDocuments(listOf(document.copy(version = 1)))

    private fun Connection.insertMultipleDocuments(documents: List<Document>): Statement =
        createStatement(table.insertSql(documents.size)).bindDocuments(documents)

    private fun Connection.updateSingleDocument(document: Document, body: String?, deleted: Long?): Statement =
        createStatement(table.updateSingleSql)
            .bindNullable(0, body)
            .bindNullable(1, deleted)
            .bind(2, partitionHash(document.id.partitionKey))
            .bind(3, document.id.partitionKey)
            .bind(4, document.id.sortKey)
            .bind(5, document.version + 1)

    private fun Connection.updateMultipleDocuments(documents: List<Document>): Statement =
        createStatement(table.updateSql(documents.size)).bindDocuments(documents)

    private fun Statement.bindKeys(ids: List<DocumentKey>) = apply {
        var index = 0

        for ((partitionKey, sortKey) in ids) {
            bind(index++, partitionHash(partitionKey))
            bind(index++, partitionKey)
            bind(index++, sortKey)
        }
    }

    private fun Statement.bindDocuments(documents: List<Document>): Statement = apply {
        val expirationTime = expirationTime()
        var index = 0

        for ((id, body, version) in documents) {
            bind(index++, partitionHash(id.partitionKey))
            bind(index++, id.partitionKey)
            bind(index++, id.sortKey)
            bind(index++, version)
            bindNullable(index++, fromBody(body))

            val deleted: Long? = when {
                body != null -> null
                version == 0L -> 1L
                else -> expirationTime
            }
            bindNullable(index++, deleted)
        }
    }

    private inline fun <reified T : Any> Statement.bindNullable(index: Int, value: T?): Statement =
        if (value == null) bindNull(index, T::class.java) else bind(index, value)

    private fun fromBody(body: JsonElement?): String? {
        require(body == null || body is JsonObject) { "The document must be a valid JSON object" }
        return body?.toString()
    }

    private fun expirationTime(): Long = (clock.instant() + expiration).epochSecond

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
                .bindKeys(distinctIds)
                .asDocuments()
                .toList()
                .associateBy { it.id }
        }

        for (id in idList) {
            emit(documents[id] ?: Document(id, null, 0))
        }
    }

    //endregion

    //region getRange

    /**
     * Retrieves the documents of a partition, sorted by sort key, whose sort key is in the given range. Deleted
     * documents are not returned.
     */
    override fun getRange(
        partitionKey: String,
        sortKeyRange: SortKeyRange,
        direction: SortDirection,
    ): Flow<Document> {
        val conditions = StringBuilder("$PARTITION_HASH = ? AND $PARTITION_KEY = ?")
        val values = mutableListOf<Any>(partitionHash(partitionKey), partitionKey)

        when (val start = sortKeyRange.start) {
            is SortKeyBound.Inclusive -> conditions.append(" AND $SORT_KEY >= ?").also { values += start.value }
            is SortKeyBound.Exclusive -> conditions.append(" AND $SORT_KEY > ?").also { values += start.value }
            SortKeyBound.Unbounded -> {}
        }

        when (val end = sortKeyRange.end) {
            is SortKeyBound.Inclusive -> conditions.append(" AND $SORT_KEY <= ?").also { values += end.value }
            is SortKeyBound.Exclusive -> conditions.append(" AND $SORT_KEY < ?").also { values += end.value }
            SortKeyBound.Unbounded -> {}
        }

        val order: String = if (direction == SortDirection.ASCENDING) "ASC" else "DESC"

        return query("$conditions ORDER BY $SORT_KEY $order", *values.toTypedArray())
    }

    //endregion

    //region query

    /**
     * Finds the documents matching the given condition, which is a SQL expression following the `WHERE` keyword and
     * referring to the columns `partition_hash`, `partition_key`, `sort_key`, `version` and `body`. The values of the
     * parameters `?` are given by [values], in that order. Deleted documents are not returned.
     */
    fun query(where: String, vararg values: Any, configure: Statement.() -> Unit = { }): Flow<Document> = flow {
        withConnection { connection ->
            val documents: Flow<Document> = connection.createStatement("${table.selectSql} WHERE $where")
                .apply {
                    values.forEachIndexed { index, value -> bind(index, value) }
                    configure()
                }
                .asDocuments()
                .filter { it.body != null }

            emitAll(documents)
        }
    }

    //endregion

    //region createTable

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
        .map { result -> result.rowsUpdated.awaitFirst() }
        .fold(0L) { sum, count -> sum + count }

    private fun Statement.asDocuments(): Flow<Document> = flow {
        execute().asFlow().collect { result ->
            emitAll(result.map(RowMapper::toDocument).asFlow())
        }
    }
}

private const val DUPLICATE_ENTRY = 1062
private const val LOCK_WAIT_TIMEOUT = 1205
private const val DEADLOCK = 1213
private const val WRITE_CONFLICT = 9007

private val conflictCodes: Set<Int> = setOf(LOCK_WAIT_TIMEOUT, DEADLOCK, WRITE_CONFLICT)
private val primaryKeyViolationMessage = Regex("""^Duplicate entry .* for key '(?:[^']*\.)?PRIMARY'$""")

/**
 * Returns whether the error is a duplicate entry for the primary key, as opposed to another unique index.
 */
private fun R2dbcException.isPrimaryKeyViolation(): Boolean =
    errorCode == DUPLICATE_ENTRY && primaryKeyViolationMessage.containsMatchIn(message.orEmpty())

/**
 * Returns whether the error means that the document has been written by a concurrent transaction. A duplicate entry
 * on a unique index other than the primary key is not a conflict, because it is caused by the content of the document.
 */
private fun R2dbcException.isConflict(): Boolean = errorCode in conflictCodes || isPrimaryKeyViolation()
