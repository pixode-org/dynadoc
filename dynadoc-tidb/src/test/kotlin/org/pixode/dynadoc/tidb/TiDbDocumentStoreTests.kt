package org.pixode.dynadoc.tidb

import io.asyncer.r2dbc.mysql.MySqlConnectionConfiguration
import io.asyncer.r2dbc.mysql.MySqlConnectionFactory
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.R2dbcException
import io.r2dbc.spi.Result
import io.r2dbc.spi.Statement
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.publish
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.UpdateConflictException
import org.pixode.dynadoc.core.getDocument
import org.pixode.dynadoc.core.parseDocument
import org.pixode.dynadoc.core.updateDocuments
import org.pixode.dynadoc.test.DocumentStoreTests
import org.pixode.dynadoc.test.JSON_1
import org.pixode.dynadoc.test.JSON_2
import org.pixode.dynadoc.test.JSON_3
import org.pixode.dynadoc.test.JSON_4
import org.pixode.dynadoc.test.JSON_5
import org.pixode.dynadoc.test.assertDocument
import org.pixode.dynadoc.test.assertDocuments
import org.reactivestreams.Publisher
import org.testcontainers.images.builder.Transferable
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.tidb.TiDBContainer
import reactor.core.publisher.Flux

private const val WRITE_CONFLICT = 9007
private const val DUPLICATE_ENTRY = 1062

private const val PARTITION_FILTER = "$PARTITION_HASH = CRC32(?) AND $PARTITION_KEY = ?"

// A test waiting for a lock would otherwise never complete
@Timeout(120, unit = TimeUnit.SECONDS)
@Testcontainers
class TiDbDocumentStoreTests : DocumentStoreTests() {
    override val store: TiDbDocumentStore = TiDbDocumentStore(connectionFactory, TABLE)
    private val longId: DocumentKey = DocumentKey(partitionKey, "A".repeat(MAX_KEY_LENGTH + 1))

    //region updateDocuments

    //region updateDocuments: Special cases

    @Test
    fun updateDocuments_noReservedFields() = runBlocking {
        val json = """ {"partition_key":"a","sort_key":"b","version":3,"body":null} """
        updateDocument(json, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], json, 1)
    }

    @Test
    fun updateDocuments_singleDocumentServerError() = runBlocking {
        // Rejected by the server, which doesn't allow keys longer than MAX_KEY_LENGTH
        assertThrows<R2dbcException> {
            updateDocument(longId, JSON_1, 0)
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @Test
    fun updateDocuments_multipleDocumentsServerError() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        assertThrows<R2dbcException> {
            store.updateDocuments(
                parseDocument(ids[0], JSON_2, 1),
                parseDocument(ids[1], JSON_3, 0),
                parseDocument(longId, JSON_4, 0),
            )
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], JSON_1, 1)
        assertDocument(document2, ids[1], null, 0)
    }

    //endregion

    //region updateDocuments: Unique constraints

    @Test
    fun updateDocuments_uniqueIndexViolationInsertSingleDocument() = runBlocking {
        val uniqueValue = UUID.randomUUID().toString()
        updateDocument(ids[0], """ {"unique":"$uniqueValue"} """, 0)

        val exception = assertThrows<R2dbcException> {
            updateDocument(ids[1], """ {"unique":"$uniqueValue"} """, 0)
        }

        val document = store.getDocument(ids[1])

        assertEquals(DUPLICATE_ENTRY, exception.errorCode)
        assertDocument(document, ids[1], null, 0)
    }

    @Test
    fun updateDocuments_uniqueIndexViolationUpdateSingleDocument() = runBlocking {
        val uniqueValue = UUID.randomUUID().toString()
        updateDocument(ids[0], """ {"unique":"$uniqueValue"} """, 0)
        updateDocument(ids[1], JSON_1, 0)

        val exception = assertThrows<R2dbcException> {
            updateDocument(ids[1], """ {"unique":"$uniqueValue"} """, 1)
        }

        val document = store.getDocument(ids[1])

        assertEquals(DUPLICATE_ENTRY, exception.errorCode)
        assertDocument(document, ids[1], JSON_1, 1)
    }

    @Test
    fun updateDocuments_uniqueIndexViolationInsertMultipleDocuments() = runBlocking {
        val uniqueValue = UUID.randomUUID().toString()
        val json1 = """ {"unique":"$uniqueValue","a":1} """
        val json3 = """ {"unique":"$uniqueValue","a":2} """
        updateDocument(ids[0], json1, 0)

        val exception = assertThrows<R2dbcException> {
            store.updateDocuments(
                parseDocument(ids[1], JSON_2, 0),
                parseDocument(ids[2], json3, 0),
            )
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])
        val document3 = store.getDocument(ids[2])

        assertEquals(DUPLICATE_ENTRY, exception.errorCode)
        assertDocument(document1, ids[0], json1, 1)
        assertDocument(document2, ids[1], null, 0)
        assertDocument(document3, ids[2], null, 0)
    }

    @Test
    fun updateDocuments_uniqueIndexViolationUpdateMultipleDocuments() = runBlocking {
        val uniqueValue = UUID.randomUUID().toString()
        val json1 = """ {"unique":"$uniqueValue","a":1} """
        val json3 = """ {"unique":"$uniqueValue","a":2} """
        updateDocument(ids[0], json1, 0)
        updateDocument(ids[1], JSON_1, 0)
        updateDocument(ids[2], JSON_2, 0)

        val exception = assertThrows<R2dbcException> {
            store.updateDocuments(
                parseDocument(ids[1], JSON_3, 1),
                parseDocument(ids[2], json3, 1),
            )
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])
        val document3 = store.getDocument(ids[2])

        assertEquals(DUPLICATE_ENTRY, exception.errorCode)
        assertDocument(document1, ids[0], json1, 1)
        assertDocument(document2, ids[1], JSON_1, 1)
        assertDocument(document3, ids[2], JSON_2, 1)
    }

    //endregion

    //region updateDocuments: Concurrent transactions

    @Test
    fun updateDocuments_concurrentUpdateSingleDocument() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        val exception: R2dbcException? = withConcurrentTransaction(parseDocument(ids[0], JSON_2, 1)) {
            store.updateDocuments(parseDocument(ids[0], JSON_3, 1))
        }

        val document = store.getDocument(ids[0])

        assertEquals(WRITE_CONFLICT, exception?.errorCode)
        assertDocument(document, ids[0], JSON_3, 2)
    }

    @Test
    fun updateDocuments_concurrentUpdateMultipleDocuments() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)
        updateDocument(ids[1], JSON_2, 0)

        val exception: R2dbcException? = withConcurrentTransaction(parseDocument(ids[1], JSON_3, 1)) {
            store.updateDocuments(
                parseDocument(ids[0], JSON_4, 1),
                parseDocument(ids[1], JSON_5, 1),
            )
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertEquals(WRITE_CONFLICT, exception?.errorCode)
        assertDocument(document1, ids[0], JSON_4, 2)
        assertDocument(document2, ids[1], JSON_5, 2)
    }

    @Test
    fun updateDocuments_concurrentInsertSingleDocument() = runBlocking {
        val exception: R2dbcException? = withConcurrentTransaction(parseDocument(ids[0], JSON_1, 0)) {
            store.updateDocuments(parseDocument(ids[0], JSON_2, 0))
        }

        val document = store.getDocument(ids[0])

        assertEquals(WRITE_CONFLICT, exception?.errorCode)
        assertDocument(document, ids[0], JSON_2, 1)
    }

    @Test
    fun updateDocuments_concurrentInsertMultipleDocuments() = runBlocking {
        val exception: R2dbcException? = withConcurrentTransaction(parseDocument(ids[1], JSON_1, 0)) {
            store.updateDocuments(
                parseDocument(ids[0], JSON_2, 0),
                parseDocument(ids[1], JSON_3, 0),
            )
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertEquals(WRITE_CONFLICT, exception?.errorCode)
        assertDocument(document1, ids[0], JSON_2, 1)
        assertDocument(document2, ids[1], JSON_3, 1)
    }

    @Test
    fun updateDocuments_concurrentInsertCheckedDocument() = runBlocking {
        val exception: R2dbcException? = withConcurrentTransaction(parseDocument(ids[1], JSON_1, 0)) {
            store.updateDocuments(
                updatedDocuments = listOf(parseDocument(ids[0], JSON_2, 0)),
                checkedDocuments = listOf(parseDocument(ids[1], JSON_3, 0)),
            )
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertEquals(WRITE_CONFLICT, exception?.errorCode)
        assertDocument(document1, ids[0], JSON_2, 1)
        assertDocument(document2, ids[1], null, 0)
    }

    @ParameterizedTest
    @ValueSource(strings = ["update", "insert", "check", "checkMissing"])
    fun updateDocuments_concurrentCommit(mode: String) = runBlocking {
        val exists: Boolean = mode == "update" || mode == "check"
        val version: Long = if (exists) 1 else 0

        updateDocument(ids[0], JSON_1, 0)
        if (exists) {
            updateDocument(ids[1], JSON_2, 0)
        }

        // The second document is written by a concurrent transaction after it has been read by the update, but before
        // the update is committed, so the update can't be committed
        val concurrentStore = TiDbDocumentStore(
            connectionFactory = connectionFactory.beforeCommit {
                updateDocument(ids[1], JSON_3, version)
            },
            tableName = TABLE,
        )

        val exception: UpdateConflictException = assertThrows {
            when (mode) {
                "update", "insert" -> concurrentStore.updateDocuments(
                    parseDocument(ids[0], JSON_4, 1),
                    parseDocument(ids[1], JSON_5, version),
                )

                else -> concurrentStore.updateDocuments(
                    updatedDocuments = listOf(parseDocument(ids[0], JSON_4, 1)),
                    checkedDocuments = listOf(parseDocument(ids[1], JSON_5, version)),
                )
            }
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        // The database doesn't indicate which document caused the conflict
        assertEquals(ids[0], exception.id)
        assertDocument(document1, ids[0], JSON_1, 1)
        assertDocument(document2, ids[1], JSON_3, version + 1)
    }

    @ParameterizedTest
    @ValueSource(strings = ["update", "insert"])
    fun updateDocuments_concurrentLock(mode: String) = runBlocking {
        if (mode == "update") {
            updateDocument(ids[0], JSON_1, 0)
        }
        val version: Long = if (mode == "update") 1 else 0

        // The connections of this store only wait for a second when a document is locked
        val waitingStore = TiDbDocumentStore(createConnectionFactory("innodb_lock_wait_timeout=1"), TABLE)

        // The document is locked by a concurrent transaction until it is committed, so the update of a single
        // document waits until its lock wait timeout, which the database reports as an error
        val exception: R2dbcException? =
            withConcurrentTransaction(parseDocument(ids[0], JSON_2, version), "PESSIMISTIC") {
                val conflict: UpdateConflictException = assertThrows {
                    waitingStore.updateDocuments(parseDocument(ids[0], JSON_3, version))
                }

                assertEquals(ids[0], conflict.id)
            }

        val document = store.getDocument(ids[0])

        assertEquals(null, exception)
        assertDocument(document, ids[0], JSON_2, version + 1)
    }

    //endregion

    /**
     * Executes [block] while a concurrent transaction writing [document] is in progress, then commits the concurrent
     * transaction, and returns the error raised by the commit, if any. The concurrent transaction is optimistic by
     * default, so it doesn't lock the document. A pessimistic transaction locks it until it is committed.
     */
    private suspend fun withConcurrentTransaction(
        document: Document,
        transactionMode: String = "OPTIMISTIC",
        block: suspend () -> Unit,
    ): R2dbcException? {
        val connection: Connection = connectionFactory.create().awaitSingle()

        try {
            connection.createStatement("BEGIN $transactionMode").execute().awaitSingle().rowsUpdated.awaitFirstOrNull()

            connection
                .createStatement(
                    "INSERT INTO $TABLE ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY, $VERSION, $BODY) " +
                    "VALUES (CRC32(?), ?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE $VERSION = VALUES($VERSION), $BODY = VALUES($BODY)",
                )
                .bind(0, document.id.partitionKey)
                .bind(1, document.id.partitionKey)
                .bind(2, document.id.sortKey)
                .bind(3, document.version + 1)
                .bind(4, checkNotNull(document.body).toString())
                .execute()
                .awaitSingle()
                .rowsUpdated
                .awaitFirstOrNull()

            block()

            return try {
                connection.createStatement("COMMIT").execute().awaitSingle().rowsUpdated.awaitFirstOrNull()
                null
            } catch (exception: R2dbcException) {
                exception
            }
        } finally {
            connection.close().awaitFirstOrNull()
        }
    }

    /**
     * Returns a connection factory whose connections execute [action] right before they commit a transaction.
     */
    private fun ConnectionFactory.beforeCommit(action: suspend () -> Unit): ConnectionFactory {
        val factory: ConnectionFactory = this

        return object : ConnectionFactory by factory {
            override fun create(): Publisher<out Connection> =
                Flux.from(factory.create()).map { connection ->
                    object : Connection by connection {
                        override fun createStatement(sql: String): Statement {
                            val statement: Statement = connection.createStatement(sql)

                            // Only this statement has no parameter, so it is not replaced by another statement when
                            // its parameters are bound
                            return if (sql == "COMMIT") {
                                object : Statement by statement {
                                    override fun execute(): Publisher<out Result> =
                                        Flux.from(publish<Any> { action() }).thenMany(Flux.from(statement.execute()))
                                }
                            } else {
                                statement
                            }
                        }
                    }
                }
        }
    }

    //endregion

    //region query

    @Test
    fun query_filterBody() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"a":$i} """, 0)
        }
        store.updateDocuments(*documents.toTypedArray())

        val result = store.query(
            "$PARTITION_FILTER AND $BODY->'$.a' > ? ORDER BY $SORT_KEY",
            partitionKey,
            partitionKey,
            4,
        )

        assertDocuments(result.toList(), documents.slice(5..9))
    }

    @Test
    fun query_filterAcrossPartitions() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(
                id = DocumentKey("${partitionKey}_$i", "0000"),
                body = """ {"b":"value $i"} """,
                version = 0,
            )
        }
        store.updateDocuments(*documents.toTypedArray())

        val result = store.query(
            "$PARTITION_KEY >= ? AND $PARTITION_KEY < ? AND $BODY->>'$.b' <= ? ORDER BY $PARTITION_KEY",
            partitionKey,
            "${partitionKey}a",
            "value 4",
        )

        assertDocuments(result.toList(), documents.slice(0..4))
    }

    @Test
    fun query_multipleBatches() = runBlocking {
        val documents = (100..399).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"b":$i} """, 0)
        }
        documents.chunked(100).forEach { chunk -> store.updateDocuments(*chunk.toTypedArray()) }

        val result = store.query(
            "$PARTITION_FILTER AND $SORT_KEY >= ? AND $SORT_KEY <= ? ORDER BY $SORT_KEY",
            partitionKey,
            partitionKey,
            "ABC0120",
            "ABC0380",
        ) {
            fetchSize(7)
        }

        assertDocuments(result.toList(), documents.slice(20..280))
    }

    @Test
    fun query_deletedDocuments() = runBlocking {
        updateDocument(DocumentKey(partitionKey, "A"), JSON_1, 0)
        updateDocument(DocumentKey(partitionKey, "B"), JSON_2, 0)
        updateDocument(DocumentKey(partitionKey, "A"), null, 1)
        // A document that is checked but doesn't exist
        store.updateDocuments(emptyList(), listOf(Document(DocumentKey(partitionKey, "C"), null, 0)))

        val result = store.query("$PARTITION_FILTER ORDER BY $SORT_KEY", partitionKey, partitionKey).toList()

        assertEquals(1, result.size)
        assertDocument(result[0], DocumentKey(partitionKey, "B"), JSON_2, 1)
    }

    //endregion

    //region createTable

    @Test
    fun createTable_qualifiedName() = runBlocking {
        val otherStore = TiDbDocumentStore(connectionFactory, "$DATABASE.tests_other")

        otherStore.createTable()
        // The table already exists
        otherStore.createTable()
        otherStore.updateDocuments(parseDocument(ids[0], JSON_1, 0))

        val document = otherStore.getDocument(ids[0])
        val otherDocument = store.getDocument(ids[0])

        assertDocument(document, ids[0], JSON_1, 1)
        assertDocument(otherDocument, ids[0], null, 0)
    }

    //endregion

    //region Setup

    private companion object Setup {
        const val TABLE = "tests"
        const val DATABASE = "test"
        const val UNIQUE_INDEX = "tests_unique"

        // A single statement waits for the documents locked by another transaction, instead of only detecting the conflict
        // when it is committed, so that an update can be made to wait until its lock wait timeout
        private val TIDB_CONFIGURATION = """
            [pessimistic-txn]
            pessimistic-auto-commit = true
        """.trimIndent()

        lateinit var connectionFactory: ConnectionFactory

        @JvmStatic
        @Container
        private val container = TiDBContainer("pingcap/tidb:v8.5.3").apply {
            withCopyToContainer(Transferable.of(TIDB_CONFIGURATION), "/etc/tidb.toml")
            withCommand("--config=/etc/tidb.toml")
        }

        @BeforeAll
        @JvmStatic
        fun globalSetup() {
            require(container.isRunning()) { container.logs }
            connectionFactory = createConnectionFactory()

            runBlocking {
                TiDbDocumentStore(connectionFactory, TABLE).createTable()
                createUniqueIndex()
            }
        }

        /**
         * Creates a unique index on the top level `unique` field of the body. TiDB doesn't support partial indexes,
         * but the documents without this field have a `NULL` index value, which a unique index doesn't constrain.
         */
        private suspend fun createUniqueIndex() {
            val connection: Connection = connectionFactory.create().awaitSingle()

            try {
                connection
                    .createStatement(
                        "CREATE UNIQUE INDEX $UNIQUE_INDEX ON $TABLE " +
                        "((CAST($BODY->>'$.unique' AS CHAR($MAX_KEY_LENGTH)) COLLATE utf8mb4_bin))",
                    )
                    .execute()
                    .awaitSingle()
                    .rowsUpdated
                    .awaitFirstOrNull()
            } finally {
                connection.close().awaitFirstOrNull()
            }
        }

        /**
         * Creates a connection factory whose connections have the given session variables, in addition to the ones
         * used by all the tests.
         */
        fun createConnectionFactory(vararg sessionVariables: String): ConnectionFactory =
            // The statements are prepared on the server, which lets TiDB cache their plans
            MySqlConnectionFactory.from(
                MySqlConnectionConfiguration.builder()
                    .host(container.host)
                    .port(container.getMappedPort(4000))
                    .user(container.username)
                    .database(DATABASE)
                    .useServerPrepareStatement()
                    // TiDB only caches the plans of the statements reading or writing several keys with this setting
                    .sessionVariables("tidb_opt_fix_control='44830:ON'", *sessionVariables)
                    .build(),
            )
    }

    //endregion
}
