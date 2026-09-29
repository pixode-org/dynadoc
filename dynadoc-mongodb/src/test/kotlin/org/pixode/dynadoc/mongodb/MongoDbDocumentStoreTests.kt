package org.pixode.dynadoc.mongodb

import com.mongodb.MongoCommandException
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.Updates
import com.mongodb.kotlin.client.coroutine.MongoClient
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.stream.Stream
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.bson.BsonDocument
import org.bson.BsonElement
import org.bson.BsonMaximumSizeExceededException
import org.bson.BsonString
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
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
import org.pixode.dynadoc.mongodb.BsonMapper
import org.pixode.dynadoc.mongodb.MongoDbDocumentStoreTests.MethodSources.PREFIX
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.mongodb.MongoDBContainer

private const val JSON_1 = """ {"abc":"def"} """
private const val JSON_2 = """ {"ghi":"jkl"} """
private const val JSON_3 = """ {"mno":"pqr"} """
private val JSON_17MB = """ {"key":"${"a".repeat(17 * 1024 * 1024)}"} """

@Testcontainers
class MongoDbDocumentStoreTests {
    private val store: MongoDbDocumentStore = MongoDbDocumentStore(client, DATABASE, COLLECTION)
    private val partitionKey: String = UUID.randomUUID().toString()
    private val ids: List<DocumentKey> = (0..10).map { i -> DocumentKey("${partitionKey}_$i", "0000") }
    private val bsonMapper = BsonMapper(Duration.ZERO, Clock.systemUTC())

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
            """ { "a":1, "$ID":2 } """,
            """ { "a":1, "$VERSION":2 } """,
            """ { "a":1, "$DELETED":2 } """,
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

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_conflictDocumentDoesNotExist(checkOnly: Boolean) = runBlocking {
        val exception = assertThrows<UpdateConflictException> {
            if (checkOnly) {
                checkDocument(10)
            } else {
                updateDocument(JSON_2, 10)
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
    fun updateDocuments_singleDocumentMongoDbError() = runBlocking {
        assertThrows<BsonMaximumSizeExceededException> {
            updateDocument(JSON_17MB, 0)
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
                parseDocument(ids[0], """ {"v":"1"} """, 1),
                parseDocument(ids[2], """ {"v":"2"} """, 0),
            ),
            checkedDocuments = listOf(
                parseDocument(ids[1], """ {"v":"3"} """, 1),
                parseDocument(ids[3], """ {"v":"4"} """, 0),
            ),
        )

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])
        val document3 = store.getDocument(ids[2])
        val document4 = store.getDocument(ids[3])

        assertDocument(document1, ids[0], """ {"v":"1"} """, 2)
        assertDocument(document2, ids[1], JSON_2, 1)
        assertDocument(document3, ids[2], """ {"v":"2"} """, 1)
        assertDocument(document4, ids[3], null, 0)
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
        updateDocument(ids[1], JSON_2, 0)

        val exception = assertThrows<UpdateConflictException> {
            store.updateDocuments(
                updatedDocuments = listOf(parseDocument(ids[0], JSON_1, 0)),
                checkedDocuments = listOf(parseDocument(ids[1], JSON_3, 0)),
            )
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], null, 0)
        assertDocument(document2, ids[1], JSON_2, 1)
        assertEquals(ids[1], exception.id)
    }

    @Test
    fun updateDocuments_multipleDocumentsConcurrentTransaction() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)
        updateDocument(ids[1], JSON_2, 0)

        client.startSession().use { session ->
            // Modify the second document in a transaction that stays open
            session.startTransaction()
            client.getDatabase(DATABASE).getCollection<BsonDocument>(COLLECTION).updateOne(
                clientSession = session,
                filter = Filters.eq("_id", bsonMapper.fromDocumentKey(ids[1])),
                update = Updates.set("ghi", "concurrent"),
            )

            assertThrows<UpdateConflictException> {
                store.updateDocuments(
                    parseDocument(ids[0], JSON_3, 1),
                    parseDocument(ids[1], JSON_3, 1),
                )
            }

            session.abortTransaction()
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], JSON_1, 1)
        assertDocument(document2, ids[1], JSON_2, 1)
    }

    @Test
    fun updateDocuments_multipleDocumentsMongoDbError() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        assertThrows<MongoCommandException> {
            store.updateDocuments(
                parseDocument(ids[0], JSON_2, 1),
                parseDocument(ids[1], JSON_17MB, 0),
            )
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], JSON_1, 1)
        assertDocument(document2, ids[1], null, 0)
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

    //endregion

    //region find

    @Test
    fun find_filterLocalKey() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"a":$i} """, 0)
        }
        store.updateDocuments(*documents.toTypedArray())

        val result = store.find(
            Filters.and(
                Filters.eq("$ID.$PARTITION_KEY", partitionKey),
                Filters.gte("$ID.$LOCAL_KEY", "ABC03"),
                Filters.lte("$ID.$LOCAL_KEY", "ABC05"),
            ),
        ) {
            sort(Sorts.ascending("$ID.$LOCAL_KEY"))
        }

        assertDocuments(result.toList(), documents.slice(3..5))
    }

    @Test
    fun find_filterNonKey() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"a":$i} """, 0)
        }
        store.updateDocuments(*documents.toTypedArray())

        val result = store.find(
            Filters.and(
                Filters.eq("$ID.$PARTITION_KEY", partitionKey),
                Filters.gt("a", 4),
            ),
        ) {
            sort(Sorts.ascending("$ID.$LOCAL_KEY"))
        }

        assertDocuments(result.toList(), documents.slice(5..9))
    }

    @Test
    fun find_filterAcrossPartitions() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(
                id = DocumentKey("${partitionKey}_$i", "0000"),
                body = """ {"b":"value $i"} """,
                version = 0,
            )
        }
        store.updateDocuments(*documents.toTypedArray())

        val result = store.find(
            Filters.and(
                Filters.regex("$ID.$PARTITION_KEY", "^$partitionKey"),
                Filters.gte("b", "val"),
                Filters.lte("b", "value 4"),
            ),
        )

        assertDocuments(result.toList().sortedBy { it.id.partitionKey }, documents.slice(0..4))
    }

    @Test
    fun find_multipleBatches() = runBlocking {
        val documents = (100..199).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"b":$i} """, 0)
        }
        documents.forEach { document -> store.updateDocuments(document) }

        val result = store.find(
            Filters.and(
                Filters.eq("$ID.$PARTITION_KEY", partitionKey),
                Filters.gte("$ID.$LOCAL_KEY", "ABC0120"),
                Filters.lte("$ID.$LOCAL_KEY", "ABC0180"),
            ),
        ) {
            sort(Sorts.ascending("$ID.$LOCAL_KEY"))
            batchSize(7)
        }

        assertDocuments(result.toList(), documents.slice(20..80))
    }

    //endregion

    //region MethodSources

    object MethodSources {
        const val PREFIX: String = $$"org.pixode.dynadoc.mongodb.MongoDbDocumentStoreTests$MethodSources"

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
                """ 1234567890.0987654321 """,
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

    private fun assertDocuments(actual: List<Document>, expected: List<Document>) {
        assertEquals(expected.size, actual.size)

        repeat(actual.size) { i ->
            assertDocument(actual[i], expected[i].id, expected[i].body.toString(), 1)
        }
    }

    //endregion

    //region Setup

    private companion object Setup {
        const val DATABASE = "dynadoc"
        const val COLLECTION = "tests"

        lateinit var client: MongoClient

        @JvmStatic
        @Container
        private val container = MongoDBContainer("mongo:8.0").withReplicaSet()

        @BeforeAll
        @JvmStatic
        fun globalSetup() {
            require(container.isRunning()) { container.logs }
            client = MongoClient.create(container.replicaSetUrl)

            runBlocking {
                MongoDbDocumentStore(client, DATABASE, COLLECTION).createCollection()
            }
        }
    }

    //endregion
}
