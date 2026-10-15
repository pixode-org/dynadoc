package org.pixode.dynadoc.dynamodb

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.DynamoDbException
import aws.smithy.kotlin.runtime.auth.awscredentials.Credentials
import aws.smithy.kotlin.runtime.net.url.Url
import kotlin.random.Random
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.getDocument
import org.pixode.dynadoc.core.parseDocument
import org.pixode.dynadoc.core.updateDocuments
import org.pixode.dynadoc.test.DocumentStoreTests
import org.pixode.dynadoc.test.JSON_1
import org.pixode.dynadoc.test.JSON_2
import org.pixode.dynadoc.test.assertDocument
import org.pixode.dynadoc.test.assertDocuments
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

private val JSON_1MB = """ {"key":"${"a".repeat(1024 * 1024)}"} """
private val STRING_100KB = "a".repeat(100 * 1024)

@Testcontainers
class DynamoDbDocumentStoreTests : DocumentStoreTests() {
    override val store: DynamoDbDocumentStore = DynamoDbDocumentStore(client, "tests")

    //region updateDocuments

    @ParameterizedTest
    @ValueSource(
        strings = [
            """ { "a":1, "$PARTITION_KEY":2 } """,
            """ { "a":1, "$SORT_KEY":2 } """,
            """ { "a":1, "$VERSION":2 } """,
            """ { "a":1, "$DELETED":2 } """,
        ],
    )
    fun updateDocuments_reservedFields(to: String) = runBlocking {
        assertThrows<IllegalArgumentException> {
            updateDocument(to, 0)
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @Test
    fun updateDocuments_singleDocumentDynamoDbError() = runBlocking {
        assertThrows<DynamoDbException> {
            updateDocument(JSON_1MB, 0)
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @Test
    fun updateDocuments_multipleDocumentsDynamoDbError() = runBlocking {
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

    @ParameterizedTest
    @ValueSource(
        strings = [
            """ { "a": 1234567890.0987654321 } """,
            """ { "b": [ 1234567890.0987654321, 0.1234567890123456789 ] } """,
        ],
    )
    fun getDocuments_jsonDeserializationHighPrecision(json: String) = runBlocking {
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

        val result = store.query {
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

        val result = store.query {
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

        val result = store.query {
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

        val result = store.scan {
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

        val result = store.scan {
            filterExpression = "partition_key BETWEEN :start AND :end"
            expressionAttributeValues = mapOf(
                ":start" to AttributeValue.S("${partitionKey}_120"),
                ":end" to AttributeValue.S("${partitionKey}_180"),
            )
        }.toList()

        assertDocuments(result.sortedBy { it.id.partitionKey }, documents.slice(20..80))
    }

    //endregion

    //region createTable

    @Test
    fun createTable_qualifiedName() = runBlocking {
        val otherStore = DynamoDbDocumentStore(client, "tests.other")

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
