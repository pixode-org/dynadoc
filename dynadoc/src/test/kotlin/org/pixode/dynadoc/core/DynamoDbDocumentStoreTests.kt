package org.pixode.dynadoc.core

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.DynamoDbException
import aws.smithy.kotlin.runtime.auth.awscredentials.Credentials
import aws.smithy.kotlin.runtime.net.url.Url
import java.util.UUID
import java.util.stream.Stream
import kotlin.random.Random
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.pixode.dynadoc.assertDocument
import org.pixode.dynadoc.core.DynamoDbDocumentStoreTests.MethodSources.PREFIX
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

private const val JSON_1 = """ {"abc":"def"} """
private const val JSON_2 = """ {"ghi":"jkl"} """
private const val JSON_3 = """ {"mno":"pqr"} """
private val JSON_1MB = """ {"key":"${"a".repeat(1024 * 1024)}"} """
private val STRING_100KB = "a".repeat(100 * 1024)

@Testcontainers
class DynamoDbDocumentStoreTests {
    private val store: DynamoDbDocumentStore = DynamoDbDocumentStore(client, "tests")
    private val partitionKey: String = UUID.randomUUID().toString()
    private val ids: List<DocumentKey> = (0..10).map { i -> DocumentKey("${partitionKey}_$i", "0000") }

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
            """ { "a":1, "$PARTITION_KEY":2 } """,
            """ { "a":1, "$SORT_KEY":2 } """,
            """ { "a":1, "$VERSION":2 } """,
            """ { "a":1, "$DELETED":2 } """,
            """ } { """,
            """ a """,
            """ { """,
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
    fun updateDocuments_genericError() = runBlocking {
        assertThrows<DynamoDbException> {
            updateDocument(JSON_1MB, 0)
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

        assertDocument(document, ids[0], JSON_1, 1);
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

        assertDocument(document, ids[0], JSON_1, 1);
        assertEquals(ids[0], exception.id)
    }

    @Test
    fun updateDocuments_singleDocumentGenericError() = runBlocking {
        assertThrows<DynamoDbException> {
            updateDocument(ids[0], JSON_1MB, 0)
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
    fun updateDocuments_multipleDocumentsGenericError() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        assertThrows<DynamoDbException> {
            store.updateDocuments(
                parseDocument(ids[0], JSON_2, 1),
                parseDocument(ids[1], JSON_1MB, 0),
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

    //region query

    @Test
    fun query_filterSortKey() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"a":$i} """, 0)
        }
        store.updateDocuments(*documents.toTypedArray())

        val result =
            store.query {
                keyConditionExpression = "partition_key = :pk AND sort_key BETWEEN :start AND :end"
                expressionAttributeValues = mapOf(
                    ":pk" to AttributeValue.S(partitionKey),
                    ":start" to AttributeValue.S("ABC03"),
                    ":end" to AttributeValue.S("ABC05"),
                )
            }.toList()

        assertDocuments(result, documents.slice(3..5))
    }

    @Test
    fun query_filterNonKey() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"a":$i} """, 0)
        }
        store.updateDocuments(*documents.toTypedArray())

        val result =
            store.query {
                keyConditionExpression = "partition_key = :pk"
                filterExpression = "a > :start"
                expressionAttributeValues = mapOf(
                    ":pk" to AttributeValue.S(partitionKey),
                    ":start" to AttributeValue.N("4"),
                )
            }.toList()

        assertDocuments(result, documents.slice(5..9))
    }

    @Test
    fun query_pagination() = runBlocking {
        val documents = (100..199).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"b":"$STRING_100KB"} """, 0)
        }
        documents.forEach { document -> store.updateDocuments(document) }

        val result =
            store.query {
                keyConditionExpression = "partition_key = :pk AND sort_key BETWEEN :start AND :end"
                expressionAttributeValues = mapOf(
                    ":pk" to AttributeValue.S(partitionKey),
                    ":start" to AttributeValue.S("ABC0120"),
                    ":end" to AttributeValue.S("ABC0180"),
                )
            }.toList()

        assertDocuments(result, documents.slice(20..80))
    }

    //endregion

    //region scan

    @Test
    fun scan_filterAttribute() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(
                id = DocumentKey("${partitionKey}_$i", "0000"),
                body = """ {"b":"value $i"} """,
                version = 0,
            )
        }
        store.updateDocuments(*documents.toTypedArray())

        val result =
            store.scan {
                filterExpression = "b BETWEEN :start AND :end"
                expressionAttributeValues = mapOf(
                    ":start" to AttributeValue.S("val"),
                    ":end" to AttributeValue.S("value 4"),
                )
            }.toList()

        assertDocuments(result.sortedBy { it.id.partitionKey }, documents.slice(0..4))
    }

    @Test
    fun scan_pagination() = runBlocking {
        val documents = (100..199).map { i ->
            parseDocument(
                id = DocumentKey("${partitionKey}_$i", "0000"),
                body = """ {"b":"$STRING_100KB"} """,
                version = 0,
            )
        }
        documents.forEach { document -> store.updateDocuments(document) }

        val result =
            store.scan {
                filterExpression = "partition_key BETWEEN :start AND :end"
                expressionAttributeValues = mapOf(
                    ":start" to AttributeValue.S("${partitionKey}_120"),
                    ":end" to AttributeValue.S("${partitionKey}_180"),
                )
            }.toList()

        assertDocuments(result.sortedBy { it.id.partitionKey }, documents.slice(20..80))
    }

    //endregion MethodSources

    object MethodSources {
        const val PREFIX: String = "org.pixode.dynadoc.core.DynamoDbDocumentStoreTests\$MethodSources"

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
        lateinit var client: DynamoDbClient
        var port: Int = Random.nextInt(10000, 32000)

        @JvmStatic
        @Container
        private val container = GenericContainer("amazon/dynamodb-local:latest").apply {
            portBindings = listOf("$port:8000")
            setCommand("-jar DynamoDBLocal.jar -sharedDb -inMemory")
            workingDirectory = "/home/dynamodblocal"
            waitingFor(
                Wait
                    .forHttp("/")
                    .forPort(8000)
                    .forStatusCode(400),
            )
        }

        @BeforeAll
        @JvmStatic
        fun globalSetup() {
            client = DynamoDbClient {
                endpointUrl = Url.parse("http://localhost:$port")
                credentialsProvider = StaticCredentialsProvider(
                    Credentials(accessKeyId = "NONE", secretAccessKey = "NONE"),
                )
                region = "eu-west-1"
            }

            require(container.isRunning()) { container.logs }
            runBlocking {
                DynamoDbDocumentStore(client, "tests").createTable()
            }
        }
    }

    //endregion
}
