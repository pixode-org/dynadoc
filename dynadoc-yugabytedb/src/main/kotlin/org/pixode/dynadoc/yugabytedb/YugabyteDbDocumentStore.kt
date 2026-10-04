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
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.UpdateConflictException

private const val LOCK_NOT_AVAILABLE = "55P03"
private const val SERIALIZATION_FAILURE = "40001"
private const val DEADLOCK_DETECTED = "40P01"
private const val UNIQUE_VIOLATION = "23505"

// The error raised by the update function to roll back its changes when a document has been modified
private const val UPDATE_CONFLICT = "DD001"

private const val CHECK = "check"
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
 * Documents are updated by a function stored in the database (see [createTable]), so that an update is a single round
 * trip to the database, and a single transaction. The function locks the rows of the documents without waiting before
 * checking and writing them, so an update fails without waiting if any of its documents is locked by a concurrent
 * update.
 */
class YugabyteDbDocumentStore(
    private val connectionFactory: ConnectionFactory,
    tableName: String,
) : DocumentStore {

    private val rowMapper: RowMapper = RowMapper()
    private val tableNameParts: List<String> = tableName.split('.')
    private val table: String = tableNameParts.joinToString(".", transform = ::quoteIdentifier)
    private val updateFunction: String =
        (tableNameParts.dropLast(1) + "${tableNameParts.last()}_update").joinToString(".", transform = ::quoteIdentifier)

    private val partitionFilter: String = "$PARTITION_HASH = ${hash("$1")} AND $PARTITION_KEY = $1"

    private val selectSql: String = "SELECT $PARTITION_KEY, $LOCAL_KEY, $VERSION, $BODY::text AS $BODY FROM $table"
    private val updateSql: String = "SELECT $updateFunction(CAST($1 AS jsonb))"

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

        val operations: String = fromDocuments(updatedList, checkedList)

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

    /**
     * Returns the parameter of the update function, which is a JSON array with an element of the form
     * `{ partition_key, local_key, version, body, check }` for each document, where `version` is the expected version
     * of the document, `body` is the new body of the document, or null if the document is deleted, and `check`
     * indicates that the version of the document is checked without the document being modified.
     */
    private fun fromDocuments(updatedDocuments: List<Document>, checkedDocuments: List<Document>): String {
        val operations: List<JsonObject> =
            updatedDocuments.map { fromDocument(it, it.body, false) } +
                checkedDocuments.map { fromDocument(it, null, true) }

        return JsonArray(operations).toString()
    }

    private fun fromDocument(document: Document, body: JsonElement?, check: Boolean): JsonObject {
        require(body == null || body is JsonObject) {
            "The document must be a valid JSON object"
        }

        return JsonObject(
            mapOf(
                PARTITION_KEY to JsonPrimitive(document.id.partitionKey),
                LOCAL_KEY to JsonPrimitive(document.id.localKey),
                VERSION to JsonPrimitive(document.version),
                BODY to (body ?: JsonNull),
                CHECK to JsonPrimitive(check),
            )
        )
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
     *
     * The function used to update the documents of the table is created along with the table, or replaced if it
     * already exists. It has the name of the table followed by `_update`.
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

        val createTableSql: String =
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

        withConnection { connection ->
            connection.createStatement(createTableSql).rowsUpdated().toList()
            connection.createStatement(createFunctionSql()).rowsUpdated().toList()
        }
    }

    /**
     * Returns the definition of the function updating the documents described by its parameter (see [fromDocuments]).
     *
     * The function returns -1 when the documents have been updated. When a document doesn't have the expected version
     * or is locked by another transaction, none of the documents is updated, and the function returns the index of
     * that document. The function runs in the transaction of the statement calling it, so the update is atomic.
     */
    private fun createFunctionSql(): String {
        val rowFilter =
            "$PARTITION_HASH = v_partition_hash AND $PARTITION_KEY = v_partition_key AND $LOCAL_KEY = v_local_key"

        return """
            CREATE OR REPLACE FUNCTION $updateFunction(p_operations JSONB) RETURNS INT
            LANGUAGE plpgsql AS $$
            DECLARE
                v_index INT := 0;
                v_operation JSONB;
                v_partition_hash INT;
                v_partition_key TEXT;
                v_local_key TEXT;
                v_check BOOLEAN;
                v_body JSONB;
                v_expected_version BIGINT;
                v_current_version BIGINT;
                v_rows INT;
            BEGIN
                -- The changes made in this block are rolled back when the block fails
                BEGIN
                    FOR v_operation IN
                        SELECT value FROM jsonb_array_elements(p_operations) WITH ORDINALITY ORDER BY ordinality
                    LOOP
                        v_partition_key := v_operation->>'$PARTITION_KEY';
                        v_local_key := v_operation->>'$LOCAL_KEY';
                        v_partition_hash := yb_hash_code(v_partition_key);
                        v_check := (v_operation->>'$CHECK')::BOOLEAN;
                        v_body := NULLIF(v_operation->'$BODY', 'null'::JSONB);
                        v_expected_version := (v_operation->>'$VERSION')::BIGINT;
                        v_current_version := NULL;

                        -- Lock the document without waiting, so that a concurrent write to it causes a conflict
                        SELECT $VERSION INTO v_current_version FROM $table WHERE $rowFilter FOR UPDATE NOWAIT;

                        IF COALESCE(v_current_version, 0) <> v_expected_version THEN
                            RAISE EXCEPTION USING ERRCODE = '$UPDATE_CONFLICT';
                        END IF;

                        IF v_current_version IS NULL THEN
                            INSERT INTO $table ($PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY, $VERSION, $BODY)
                            VALUES (v_partition_hash, v_partition_key, v_local_key, 1, v_body)
                            ON CONFLICT DO NOTHING;

                            GET DIAGNOSTICS v_rows = ROW_COUNT;
                            IF v_rows = 0 THEN
                                RAISE EXCEPTION USING ERRCODE = '$UPDATE_CONFLICT';
                            END IF;

                            -- A checked document that doesn't exist is inserted then deleted, so that a concurrent
                            -- insert of the same document causes a conflict
                            IF v_check THEN
                                DELETE FROM $table WHERE $rowFilter;
                            END IF;
                        ELSIF NOT v_check THEN
                            UPDATE $table SET $VERSION = v_expected_version + 1, $BODY = v_body WHERE $rowFilter;
                        END IF;

                        v_index := v_index + 1;
                    END LOOP;
                EXCEPTION WHEN SQLSTATE '$UPDATE_CONFLICT' OR lock_not_available OR unique_violation THEN
                    RETURN v_index;
                END;

                RETURN -1;
            END
            $$
            """.trimIndent()
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

    private fun Statement.rowsUpdated(): Flow<Long> =
        execute().asFlow().map { result -> result.rowsUpdated.awaitFirstOrNull() ?: 0L }

    private fun <T : Any> Statement.rows(transform: (Row) -> T): Flow<T> = flow {
        execute().asFlow().collect { result ->
            emitAll(result.map { row, _ -> transform(row) }.asFlow())
        }
    }

    private fun Statement.bindKeys(ids: List<DocumentKey>): Statement {
        ids.forEachIndexed { index, id ->
            bind(index * 2, id.partitionKey).bind(index * 2 + 1, id.localKey)
        }

        return this
    }

    // Each row is read directly using an equality on the whole primary key, whereas a single condition matching all
    // the keys can result in the whole table being read
    private fun selectKeysSql(count: Int): String =
        (0 until count).joinToString(" UNION ALL ") { i ->
            val partitionKey = "$${i * 2 + 1}"
            val localKey = "$${i * 2 + 2}"

            "$selectSql WHERE $PARTITION_HASH = ${hash(partitionKey)} " +
                "AND $PARTITION_KEY = $partitionKey AND $LOCAL_KEY = $localKey"
        }

    // The hash used by YugabyteDB for hash sharding, which is evenly distributed between 0 and HASH_COUNT - 1
    private fun hash(partitionKey: String): String = "yb_hash_code($partitionKey::text COLLATE \"C\")"

    private fun quoteIdentifier(identifier: String): String = "\"${identifier.replace("\"", "\"\"")}\""
}
