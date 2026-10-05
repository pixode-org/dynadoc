package org.pixode.dynadoc.tidb

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.R2dbcException
import io.r2dbc.spi.Statement
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.stream.Stream
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.UpdateConflictException
import org.pixode.dynadoc.core.getDocument
import org.pixode.dynadoc.core.parseDocument
import org.pixode.dynadoc.core.updateDocuments
import org.pixode.dynadoc.tidb.TiDbDocumentStoreTests.MethodSources.PREFIX
import org.testcontainers.tidb.TiDBContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

private const val JSON_1 = """ {"abc":"def"} """
private const val JSON_2 = """ {"ghi":"jkl"} """
private const val JSON_3 = """ {"mno":"pqr"} """
private const val JSON_4 = """ {"stu":"vwx"} """
private const val JSON_5 = """ {"yza":"bcd"} """
private const val JSON_6 = """ {"efg":"hij"} """
private const val WRITE_CONFLICT = 9007
private const val PARTITION_FILTER = "$PARTITION_HASH = CRC32(?) AND $PARTITION_KEY = ?"

// A test waiting for a lock would otherwise never complete
@Timeout(120, unit = TimeUnit.SECONDS)
@Testcontainers
class TiDbDocumentStoreTests {
    private val store: TiDbDocumentStore = TiDbDocumentStore(connectionFactory, TABLE)
    private val partitionKey: String = UUID.randomUUID().toString()
    private val ids: List<DocumentKey> = (0..10).map { i -> DocumentKey("${partitionKey}_$i", "0000") }
    private val longId: DocumentKey = DocumentKey(partitionKey, "A".repeat(MAX_KEY_LENGTH + 1))

    //region updateDocuments

    @ParameterizedTest
    @MethodSource("$PREFIX#updateDocuments_oneArgument")
    fun updateDocuments_emptyToValue(to: String?) = runBlocking {
        updateDocument(to, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], to, 1)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#updateDocuments_twoArguments")
    fun updateDocuments_valueToValue(from: String?, to: String?) = runBlocking {
        updateDocument(from, 0)
        updateDocument(to, 1)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], to, 2)
    }

    @Test
    fun updateDocuments_emptyToCheck() = runBlocking {
        checkDocument(0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#updateDocuments_oneArgument")
    fun updateDocuments_valueToCheck(from: String?) = runBlocking {
        updateDocument(from, 0)
        checkDocument(1)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], from, 1)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#updateDocuments_oneArgument")
    fun updateDocuments_checkToValue(to: String?) = runBlocking {
        checkDocument(0)
        updateDocument(to, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], to, 1)
    }

    @Test
    fun updateDocuments_checkToCheck() = runBlocking {
        checkDocument(0)
        checkDocument(0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @Test
    fun updateDocuments_noUpdate() = runBlocking {
        store.updateDocuments(
            updatedDocuments = emptyList(),
            checkedDocuments = emptyList(),
        )
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            """ "a" """,
            """ 10 """,
            """ true """,
            """ false """,
            """ null """,
            """ ["a"] """,
            """ a """,
        ],
    )
    fun updateDocuments_invalidJson(to: String) = runBlocking {
        assertThrows<IllegalArgumentException> {
            updateDocument(to, 0)
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @Test
    fun updateDocuments_noReservedFields() = runBlocking {
        val json = """ {"partition_key":"a","local_key":"b","version":3,"body":null} """
        updateDocument(json, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], json, 1)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_conflictDocumentDoesNotExist(checkOnly: Boolean) = runBlocking {
        val exception = assertThrows<UpdateConflictException> {
            if (checkOnly) {
                checkDocument(10)
            } else {
                updateDocument(JSON_1, 10)
            }
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
        assertEquals(ids[0], exception.id)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_conflictWrongVersion(checkOnly: Boolean) = runBlocking {
        updateDocument(JSON_1, 0)

        val exception = assertThrows<UpdateConflictException> {
            if (checkOnly) {
                checkDocument(10)
            } else {
                updateDocument(JSON_2, 10)
            }
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], JSON_1, 1)
        assertEquals(ids[0], exception.id)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_conflictDocumentAlreadyExists(checkOnly: Boolean) = runBlocking {
        updateDocument(JSON_1, 0)

        val exception = assertThrows<UpdateConflictException> {
            if (checkOnly) {
                checkDocument(0)
            } else {
                updateDocument(JSON_2, 0)
            }
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], JSON_1, 1)
        assertEquals(ids[0], exception.id)
    }

    @Test
    fun updateDocuments_conflictDeletedDocument() = runBlocking {
        updateDocument(JSON_1, 0)
        updateDocument(null, 1)

        val exception = assertThrows<UpdateConflictException> {
            updateDocument(JSON_2, 0)
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 2)
        assertEquals(ids[0], exception.id)
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
    fun updateDocuments_multipleDocumentsSuccess() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)
        updateDocument(ids[1], JSON_2, 0)

        store.updateDocuments(
            updatedDocuments = listOf(
                parseDocument(ids[0], JSON_3, 1),
                parseDocument(ids[2], JSON_4, 0),
            ),
            checkedDocuments = listOf(
                parseDocument(ids[1], JSON_5, 1),
                parseDocument(ids[3], JSON_6, 0),
            ),
        )

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])
        val document3 = store.getDocument(ids[2])
        val document4 = store.getDocument(ids[3])

        assertDocument(document1, ids[0], JSON_3, 2)
        assertDocument(document2, ids[1], JSON_2, 1)
        assertDocument(document3, ids[2], JSON_4, 1)
        assertDocument(document4, ids[3], null, 0)
    }

    @Test
    fun updateDocuments_multipleDocumentsDeleted() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        store.updateDocuments(
            parseDocument(ids[0], null, 1),
            parseDocument(ids[1], null, 0),
        )

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], null, 2)
        assertDocument(document2, ids[1], null, 1)
    }

    @Test
    fun updateDocuments_multipleDocumentsUpdatedAndChecked() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        store.updateDocuments(
            updatedDocuments = listOf(parseDocument(ids[0], JSON_2, 1)),
            checkedDocuments = listOf(parseDocument(ids[0], JSON_3, 1)),
        )

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], JSON_2, 2)
    }

    @Test
    fun updateDocuments_multipleDocumentsUpdatedAndCheckedConflict() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        val exception = assertThrows<UpdateConflictException> {
            store.updateDocuments(
                updatedDocuments = listOf(parseDocument(ids[0], JSON_2, 1)),
                checkedDocuments = listOf(parseDocument(ids[0], JSON_3, 2)),
            )
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], JSON_1, 1)
        assertEquals(ids[0], exception.id)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_multipleDocumentsConflict(checkOnly: Boolean) = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        val exception = assertThrows<UpdateConflictException> {
            if (checkOnly) {
                store.updateDocuments(
                    updatedDocuments = listOf(parseDocument(ids[0], JSON_2, 1)),
                    checkedDocuments = listOf(parseDocument(ids[1], JSON_3, 10)),
                )
            } else {
                store.updateDocuments(
                    parseDocument(ids[0], JSON_2, 1),
                    parseDocument(ids[1], JSON_3, 10),
                )
            }
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], JSON_1, 1)
        assertDocument(document2, ids[1], null, 0)
        assertEquals(ids[1], exception.id)
    }

    @Test
    fun updateDocuments_multipleDocumentsCheckExistingConflict() = runBlocking {
        updateDocument(ids[1], JSON_1, 0)

        val exception = assertThrows<UpdateConflictException> {
            store.updateDocuments(
                updatedDocuments = listOf(parseDocument(ids[0], JSON_2, 0)),
                checkedDocuments = listOf(parseDocument(ids[1], JSON_3, 0)),
            )
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], null, 0)
        assertDocument(document2, ids[1], JSON_1, 1)
        assertEquals(ids[1], exception.id)
    }

    @Test
    fun updateDocuments_largeBatch() = runBlocking {
        val documents = (0..499).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC%04d".format(i)), """ {"a":$i} """, 0)
        }
        store.updateDocuments(*documents.toTypedArray())
        store.updateDocuments(
            updatedDocuments = documents.take(250).map { it.copy(version = 1) },
            checkedDocuments = documents.drop(250).map { it.copy(version = 1) },
        )

        val result: List<Document> = store.scan(partitionKey).toList()

        assertEquals(500, result.size)
        repeat(500) { i ->
            assertDocument(result[i], documents[i].id, documents[i].body.toString(), if (i < 250) 2 else 1)
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["single", "multiple", "check"])
    fun updateDocuments_concurrentTransaction(mode: String) = runBlocking {
        updateDocument(ids[0], JSON_1, 0)
        updateDocument(ids[1], JSON_2, 0)

        // The update is committed first, so the concurrent transaction writing the same document can't be committed,
        // unless the update only checks the document
        val exception: R2dbcException? = withConcurrentTransaction(parseDocument(ids[1], JSON_3, 1)) {
            when (mode) {
                "single" -> store.updateDocuments(parseDocument(ids[1], JSON_5, 1))
                "multiple" -> store.updateDocuments(
                    parseDocument(ids[0], JSON_4, 1),
                    parseDocument(ids[1], JSON_5, 1),
                )
                else -> store.updateDocuments(
                    updatedDocuments = listOf(parseDocument(ids[0], JSON_4, 1)),
                    checkedDocuments = listOf(parseDocument(ids[1], JSON_5, 1)),
                )
            }
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertEquals(if (mode == "check") null else WRITE_CONFLICT, exception?.errorCode)
        assertDocument(document1, ids[0], if (mode == "single") JSON_1 else JSON_4, if (mode == "single") 1 else 2)
        assertDocument(document2, ids[1], if (mode == "check") JSON_3 else JSON_5, 2)
    }

    @ParameterizedTest
    @ValueSource(strings = ["single", "multiple", "check"])
    fun updateDocuments_concurrentInsert(mode: String) = runBlocking {
        // The update is committed first, so the concurrent transaction creating the same document can't be committed,
        // unless the update only checks the document
        val exception: R2dbcException? = withConcurrentTransaction(parseDocument(ids[1], JSON_1, 0)) {
            when (mode) {
                "single" -> store.updateDocuments(parseDocument(ids[1], JSON_3, 0))
                "multiple" -> store.updateDocuments(
                    parseDocument(ids[0], JSON_2, 0),
                    parseDocument(ids[1], JSON_3, 0),
                )
                else -> store.updateDocuments(
                    updatedDocuments = listOf(parseDocument(ids[0], JSON_2, 0)),
                    checkedDocuments = listOf(parseDocument(ids[1], JSON_3, 0)),
                )
            }
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertEquals(if (mode == "check") null else WRITE_CONFLICT, exception?.errorCode)
        assertDocument(document1, ids[0], if (mode == "single") null else JSON_2, if (mode == "single") 0 else 1)
        assertDocument(document2, ids[1], if (mode == "check") JSON_1 else JSON_3, 1)
    }

    @ParameterizedTest
    @ValueSource(strings = ["single", "multiple", "check", "insert"])
    fun updateDocuments_concurrentConflict(mode: String) = runBlocking {
        if (mode != "insert") {
            updateDocument(ids[0], JSON_1, 0)
        }

        // Only one of the updates of the same version of a document succeeds
        val results: List<Result<Unit>> = (1..10)
            .map { i ->
                async(Dispatchers.Default) {
                    runCatching {
                        when (mode) {
                            "single" -> store.updateDocuments(parseDocument(ids[0], """ {"a":$i} """, 1))
                            "insert" -> store.updateDocuments(parseDocument(ids[0], """ {"a":$i} """, 0))
                            "multiple" -> store.updateDocuments(
                                parseDocument(ids[0], """ {"a":$i} """, 1),
                                parseDocument(ids[i], JSON_2, 0),
                            )
                            else -> store.updateDocuments(
                                updatedDocuments = listOf(parseDocument(ids[0], """ {"a":$i} """, 1)),
                                checkedDocuments = listOf(parseDocument(ids[i], JSON_2, 0)),
                            )
                        }
                    }
                }
            }
            .map { it.await() }

        val succeeded: List<Int> = results.indices.filter { results[it].isSuccess }
        val document = store.getDocument(ids[0])

        assertEquals(1, succeeded.size, results.toString())
        results.filter { it.isFailure }.forEach { result ->
            assertInstanceOf(UpdateConflictException::class.java, result.exceptionOrNull())
        }
        assertDocument(document, ids[0], """ {"a":${succeeded[0] + 1}} """, if (mode == "insert") 1 else 2)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_concurrentChecks(existing: Boolean) = runBlocking {
        val version: Long = if (existing) 1 else 0

        repeat(30) { round ->
            val id1 = DocumentKey("${partitionKey}_$round", "A")
            val id2 = DocumentKey("${partitionKey}_$round", "B")
            if (existing) {
                store.updateDocuments(parseDocument(id1, JSON_1, 0), parseDocument(id2, JSON_1, 0))
            }

            // Each update writes the document checked by the other one, so they can't both succeed
            val results: List<Result<Unit>> = listOf(id1 to id2, id2 to id1)
                .map { (updated, checked) ->
                    async(Dispatchers.Default) {
                        runCatching {
                            store.updateDocuments(
                                updatedDocuments = listOf(parseDocument(updated, JSON_2, version)),
                                checkedDocuments = listOf(parseDocument(checked, JSON_3, version)),
                            )
                        }
                    }
                }
                .map { it.await() }

            val documents: List<Document> = store.getDocuments(listOf(id1, id2)).toList()

            assertTrue(results.count { it.isSuccess } <= 1, "Round $round: $results")
            results.filter { it.isFailure }.forEach { result ->
                assertInstanceOf(UpdateConflictException::class.java, result.exceptionOrNull())
            }
            assertEquals(results.count { it.isSuccess }, documents.count { it.version == version + 1 })
        }
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

    @Test
    fun updateDocuments_concurrentUpdates() = runBlocking {
        // The documents of each update are held in the same temporary table
        val batches: List<List<Document>> = (0..19).map { batch ->
            (0..9).map { i ->
                parseDocument(DocumentKey("${partitionKey}_$batch", "ABC0$i"), """ {"batch":$batch,"a":$i} """, 0)
            }
        }

        val results: List<Result<Unit>> = batches
            .map { documents ->
                async(Dispatchers.Default) { runCatching { store.updateDocuments(*documents.toTypedArray()) } }
            }
            .map { it.await() }

        results.forEach { result -> assertEquals(Result.success(Unit), result) }
        batches.forEachIndexed { batch, documents ->
            assertDocuments(store.scan("${partitionKey}_$batch").toList(), documents)
        }
    }

    @Test
    fun updateDocuments_trailingSpace() = runBlocking {
        // Keys that only differ by a trailing space identify different documents
        val otherId = DocumentKey("${ids[0].partitionKey} ", "${ids[0].localKey} ")
        updateDocument(ids[0], JSON_1, 0)
        updateDocument(otherId, JSON_2, 0)

        val documents: List<Document> = store.getDocuments(listOf(ids[0], otherId)).toList()

        assertDocument(documents[0], ids[0], JSON_1, 1)
        assertDocument(documents[1], otherId, JSON_2, 1)
    }

    @Test
    fun updateDocuments_specialCharacters() = runBlocking {
        val specialId = DocumentKey("$partitionKey \"'\\ é😀", "\"'\\ é😀")
        val json = """ {"a \"'\\ é":"b \"'\\ é\n"} """
        updateDocument(specialId, json, 0)
        updateDocument(specialId, json, 1)

        val document = store.getDocument(specialId)

        assertDocument(document, specialId, json, 2)
    }

    /**
     * Executes [block] while a concurrent optimistic transaction writing [document] is in progress, then commits the
     * concurrent transaction, and returns the error raised by the commit, if any.
     */
    private suspend fun withConcurrentTransaction(document: Document, block: suspend () -> Unit): R2dbcException? {
        val connection: Connection = connectionFactory.create().awaitSingle()

        try {
            connection.createStatement("BEGIN OPTIMISTIC").execute().awaitSingle().rowsUpdated.awaitFirstOrNull()
            connection
                .createStatement(
                    "INSERT INTO $TABLE ($PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY, $VERSION, $BODY) " +
                        "VALUES (CRC32(?), ?, ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE $VERSION = VALUES($VERSION), $BODY = VALUES($BODY)"
                )
                .bind(0, document.id.partitionKey)
                .bind(1, document.id.partitionKey)
                .bind(2, document.id.localKey)
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

    //endregion

    //region getDocuments

    @Test
    fun getDocuments_singleDocument() = runBlocking {
        updateDocument(JSON_1, 0)

        val documents: List<Document> = store.getDocuments(listOf(ids[0])).toList()

        assertEquals(1, documents.size)
        assertDocument(documents[0], ids[0], JSON_1, 1)
    }

    @Test
    fun getDocuments_multipleDocuments() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)
        updateDocument(ids[1], JSON_2, 0)

        val documents: List<Document> = store.getDocuments(listOf(ids[0], ids[2], ids[0], ids[1])).toList()

        assertEquals(4, documents.size)
        assertDocument(documents[0], ids[0], JSON_1, 1)
        assertDocument(documents[1], ids[2], null, 0)
        assertDocument(documents[2], ids[0], JSON_1, 1)
        assertDocument(documents[3], ids[1], JSON_2, 1)
    }

    @Test
    fun getDocuments_noDocument() = runBlocking {
        val documents: List<Document> = store.getDocuments(listOf()).toList()

        assertEquals(0, documents.size)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#getDocuments_jsonDeserialization")
    fun getDocuments_jsonDeserialization(json: String) = runBlocking {
        updateDocument(ids[0], json, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], json, 1)
    }

    @Test
    fun getDocuments_numberPrecision() = runBlocking {
        // Numbers that are not integers are stored as double-precision floating-point numbers
        val json = """ {"a":1234567890.0987654321,"b":99999999999999999999,"c":9007199254740993} """
        val storedJson = """ {"a":1234567890.0987654,"b":100000000000000000000,"c":9007199254740993} """
        updateDocument(ids[0], json, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], storedJson, 1)
    }

    //endregion

    //region scan

    @Test
    fun scan_range() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"a":$i} """, 0)
        }
        store.updateDocuments(*documents.toTypedArray())

        val result = store.scan(partitionKey, "ABC03", "ABC06")

        assertDocuments(result.toList(), documents.slice(3..5))
    }

    @Test
    fun scan_wholePartition() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"a":$i} """, 0)
        }
        // Documents with the same local keys in another partition
        val otherDocuments = (0..9).map { i ->
            parseDocument(DocumentKey("${partitionKey}_other", "ABC0$i"), """ {"b":$i} """, 0)
        }
        store.updateDocuments(*(otherDocuments + documents.reversed()).toTypedArray())

        val result = store.scan(partitionKey)

        assertDocuments(result.toList(), documents)
    }

    @Test
    fun scan_deletedDocuments() = runBlocking {
        updateDocument(DocumentKey(partitionKey, "A"), JSON_1, 0)
        updateDocument(DocumentKey(partitionKey, "B"), JSON_2, 0)
        updateDocument(DocumentKey(partitionKey, "A"), null, 1)

        val result = store.scan(partitionKey).toList()

        assertEquals(2, result.size)
        assertDocument(result[0], DocumentKey(partitionKey, "A"), null, 2)
        assertDocument(result[1], DocumentKey(partitionKey, "B"), JSON_2, 1)
    }

    @Test
    fun scan_emptyPartition() = runBlocking {
        val result = store.scan(partitionKey).toList()

        assertEquals(0, result.size)
    }

    @Test
    fun scan_byteOrder() = runBlocking {
        // Local keys are sorted by their UTF-8 encoding, regardless of the locale of the database
        val localKeys: List<String> = listOf("B", "a", "é")
        val documents = localKeys.map { parseDocument(DocumentKey(partitionKey, it), JSON_1, 0) }
        store.updateDocuments(*documents.reversed().toTypedArray())

        val result = store.scan(partitionKey)

        assertDocuments(result.toList(), documents)
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
            "$PARTITION_FILTER AND $BODY->'$.a' > ? ORDER BY $LOCAL_KEY",
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
            "$PARTITION_FILTER AND $LOCAL_KEY >= ? AND $LOCAL_KEY <= ? ORDER BY $LOCAL_KEY",
            partitionKey,
            partitionKey,
            "ABC0120",
            "ABC0380",
        ) {
            fetchSize(7)
        }

        assertDocuments(result.toList(), documents.slice(20..280))
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

    //region MethodSources

    object MethodSources {
        const val PREFIX: String = $$"org.pixode.dynadoc.tidb.TiDbDocumentStoreTests$MethodSources"

        @JvmStatic
        fun updateDocuments_oneArgument(): Stream<String?> {
            return Stream.of(
                JSON_1,
                null,
            )
        }

        @JvmStatic
        fun updateDocuments_twoArguments(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(JSON_1, JSON_2),
                Arguments.of(null, JSON_2),
                Arguments.of(JSON_1, null),
                Arguments.of(null, null),
            )
        }

        @JvmStatic
        fun getDocuments_jsonDeserialization(): Stream<String> {
            val scalars: List<String> = listOf(
                """ 1234567890.25 """,
                """ 42 """,
                """ "text" """,
                """ true """,
                """ false """,
                """ null """,
            )

            val firstLevel: List<String> = scalars.map {
                """ { "a": $it } """
            } + """ { } """

            val nestedObjects = firstLevel.map {
                """ { "b": $it } """
            }

            val arrayOfObjects = (scalars + firstLevel).map {
                val repeat = "$it, $it, $it"
                """ { "b": [ $repeat ] } """
            }

            val mixedArray = """ { "c": [ ${(scalars + firstLevel).joinToString()} ] } """

            return (firstLevel + nestedObjects + arrayOfObjects + mixedArray).stream()
        }
    }

    //endregion

    //region Helper Methods

    private suspend fun updateDocument(body: String?, version: Long) =
        updateDocument(ids[0], body, version)

    private suspend fun updateDocument(id: DocumentKey, body: String?, version: Long) =
        store.updateDocuments(parseDocument(id, body, version))

    private suspend fun checkDocument(version: Long) =
        store.updateDocuments(
            updatedDocuments = emptyList(),
            checkedDocuments = listOf(parseDocument(ids[0], """ {"ignored":"ignored"} """, version)),
        )

    /**
     * Executes a SQL statement and returns the first column of the result.
     */
    private suspend fun execute(sql: String, vararg values: Any): List<String> {
        val connection: Connection = connectionFactory.create().awaitSingle()

        try {
            val statement: Statement = connection.createStatement(sql)
            values.forEachIndexed { index, value -> statement.bind(index, value) }

            return statement.execute().asFlow()
                .map { result -> result.map { row, _ -> row.get(0, String::class.java).orEmpty() }.asFlow().toList() }
                .toList()
                .flatten()
        } finally {
            connection.close().awaitFirstOrNull()
        }
    }

    private fun assertDocuments(actual: List<Document>, expected: List<Document>) {
        assertEquals(expected.size, actual.size)

        repeat(actual.size) { i ->
            assertDocument(actual[i], expected[i].id, expected[i].body.toString(), 1)
        }
    }

    //endregion

    //region Setup

    private companion object Setup {
        const val TABLE = "tests"
        const val DATABASE = "test"

        lateinit var connectionFactory: ConnectionFactory

        @JvmStatic
        @Container
        private val container = TiDBContainer("pingcap/tidb:v8.5.3")

        @BeforeAll
        @JvmStatic
        fun globalSetup() {
            require(container.isRunning()) { container.logs }
            connectionFactory = ConnectionFactories.get(
                "r2dbc:mysql://${container.username}@${container.host}:${container.getMappedPort(4000)}/$DATABASE"
            )

            runBlocking {
                TiDbDocumentStore(connectionFactory, TABLE).createTable()
            }
        }
    }

    //endregion
}
