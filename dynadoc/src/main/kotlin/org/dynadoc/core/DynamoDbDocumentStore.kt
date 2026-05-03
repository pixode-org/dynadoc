package org.dynadoc.core

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.batchGetItem
import aws.sdk.kotlin.services.dynamodb.createTable
import aws.sdk.kotlin.services.dynamodb.model.*
import aws.sdk.kotlin.services.dynamodb.query
import aws.sdk.kotlin.services.dynamodb.scan
import aws.sdk.kotlin.services.dynamodb.transactWriteItems
import kotlinx.coroutines.flow.*
import java.time.Clock
import java.time.Duration

/**
 * Represents an implementation of the [DocumentStore] interface that relies on DynamoDB for persistence.
 */
class DynamoDbDocumentStore(
    private val client: DynamoDbClient,
    private val tableName: String,
    expiration: Duration = Duration.ofDays(30),
    clock: Clock = Clock.systemUTC()
) : DocumentStore {

    private val attributeMapper: AttributeMapper = AttributeMapper(expiration, clock)

    override suspend fun updateDocuments(updatedDocuments: Iterable<Document>, checkedDocuments: Iterable<Document>) {
        fun conditionExpression(version: Long) =
            if (version == 0L) {
                "attribute_not_exists($PARTITION_KEY)"
            } else {
                "$VERSION = :version"
            }

        fun expressionAttributeValues(version: Long) =
            if (version == 0L) {
                null
            } else {
                mapOf(":version" to AttributeValue.N(version.toString()))
            }

        val updates: Sequence<TransactWriteItem> = updatedDocuments.asSequence().map { document ->
            TransactWriteItem {
                put = Put {
                    tableName = this@DynamoDbDocumentStore.tableName
                    item = attributeMapper.fromDocument(document)
                    conditionExpression = conditionExpression(document.version)
                    expressionAttributeValues = expressionAttributeValues(document.version)
                }
            }
        }

        val checks: Sequence<TransactWriteItem> = checkedDocuments.asSequence().map { document ->
            TransactWriteItem {
                conditionCheck = ConditionCheck {
                    tableName = this@DynamoDbDocumentStore.tableName
                    key = attributeMapper.fromDocumentKey(document.id)
                    conditionExpression = conditionExpression(document.version)
                    expressionAttributeValues = expressionAttributeValues(document.version)
                }
            }
        }

        val writeItems: List<TransactWriteItem> = (updates + checks).toList()

        if (writeItems.isEmpty()) {
            return
        }

        try {
            client.transactWriteItems {
                transactItems = writeItems
            }
        } catch (exception: TransactionCanceledException) {
            val conditionCheckFailureIndex: Int = exception.cancellationReasons
                ?.indexOfFirst { it.code == "ConditionalCheckFailed" }
                ?: -1

            if (conditionCheckFailureIndex >= 0) {
                val failedItem: TransactWriteItem = writeItems[conditionCheckFailureIndex]
                val keyAttributes: Map<String, AttributeValue> =
                    checkNotNull(failedItem.put?.item ?: failedItem.conditionCheck?.key)

                throw UpdateConflictException(attributeMapper.toDocumentKey(keyAttributes))
            } else {
                throw exception
            }
        }
    }

    override fun getDocuments(ids: Iterable<DocumentKey>): Flow<Document> {
        val idList: List<DocumentKey> = ids.toList()

        if (idList.isEmpty()) {
            return flowOf()
        }

        return flow {
            val response = client.batchGetItem {
                requestItems = mapOf(
                    tableName to KeysAndAttributes {
                        keys = idList.distinct().map(attributeMapper::fromDocumentKey)
                        consistentRead = true
                    }
                )
            }

            val responses = response.responses
            if (responses != null) {
                val documents: Map<DocumentKey, Document> = responses.values
                    .flatten()
                    .map(attributeMapper::toDocument)
                    .associateBy { it.id }

                val result = idList.map { id ->
                    documents[id] ?: Document(id, null, 0)
                }

                result.asFlow().collect(this)
            }
        }
    }

    fun query(queryRequest: QueryRequest.Builder.() -> Unit): Flow<Document> = flow {
        var startKey: Map<String, AttributeValue>? = null
        while (true) {
            val currentStartKey = startKey
            val response = client.query {
                tableName = this@DynamoDbDocumentStore.tableName
                queryRequest()
                if (currentStartKey != null) {
                    exclusiveStartKey = currentStartKey
                }
            }
            for (item in response.items ?: emptyList()) {
                emit(attributeMapper.toDocument(item))
            }
            startKey = response.lastEvaluatedKey?.takeIf { it.isNotEmpty() }
            if (startKey == null) break
        }
    }

    fun scan(scanRequest: ScanRequest.Builder.() -> Unit): Flow<Document> = flow {
        var startKey: Map<String, AttributeValue>? = null
        while (true) {
            val currentStartKey = startKey
            val response = client.scan {
                tableName = this@DynamoDbDocumentStore.tableName
                scanRequest()
                if (currentStartKey != null) {
                    exclusiveStartKey = currentStartKey
                }
            }
            for (item in response.items ?: emptyList()) {
                emit(attributeMapper.toDocument(item))
            }
            startKey = response.lastEvaluatedKey?.takeIf { it.isNotEmpty() }
            if (startKey == null) break
        }
    }

    suspend fun createTable(configure: CreateTableRequest.Builder.() -> Unit = { }) {
        client.createTable {
            tableName = this@DynamoDbDocumentStore.tableName
            keySchema = listOf(
                KeySchemaElement {
                    attributeName = PARTITION_KEY
                    keyType = KeyType.Hash
                },
                KeySchemaElement {
                    attributeName = SORT_KEY
                    keyType = KeyType.Range
                }
            )
            attributeDefinitions = listOf(
                AttributeDefinition {
                    attributeName = PARTITION_KEY
                    attributeType = ScalarAttributeType.S
                },
                AttributeDefinition {
                    attributeName = SORT_KEY
                    attributeType = ScalarAttributeType.S
                }
            )
            billingMode = BillingMode.PayPerRequest
            configure()
        }
    }
}
