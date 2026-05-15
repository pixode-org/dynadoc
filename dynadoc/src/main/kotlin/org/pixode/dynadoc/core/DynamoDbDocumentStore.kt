package org.pixode.dynadoc.core

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.batchGetItem
import aws.sdk.kotlin.services.dynamodb.createTable
import aws.sdk.kotlin.services.dynamodb.getItem
import aws.sdk.kotlin.services.dynamodb.model.AttributeDefinition
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.BillingMode
import aws.sdk.kotlin.services.dynamodb.model.ConditionCheck
import aws.sdk.kotlin.services.dynamodb.model.ConditionalCheckFailedException
import aws.sdk.kotlin.services.dynamodb.model.CreateTableRequest
import aws.sdk.kotlin.services.dynamodb.model.KeySchemaElement
import aws.sdk.kotlin.services.dynamodb.model.KeyType
import aws.sdk.kotlin.services.dynamodb.model.KeysAndAttributes
import aws.sdk.kotlin.services.dynamodb.model.Put
import aws.sdk.kotlin.services.dynamodb.model.QueryRequest
import aws.sdk.kotlin.services.dynamodb.model.ScalarAttributeType
import aws.sdk.kotlin.services.dynamodb.model.ScanRequest
import aws.sdk.kotlin.services.dynamodb.model.TransactWriteItem
import aws.sdk.kotlin.services.dynamodb.model.TransactionCanceledException
import aws.sdk.kotlin.services.dynamodb.putItem
import aws.sdk.kotlin.services.dynamodb.query
import aws.sdk.kotlin.services.dynamodb.scan
import aws.sdk.kotlin.services.dynamodb.transactWriteItems
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow

/**
 * Represents an implementation of the [DocumentStore] interface that relies on DynamoDB for persistence.
 */
class DynamoDbDocumentStore(
    private val client: DynamoDbClient,
    private val tableName: String,
    private val consistentReads: Boolean = false,
    expiration: Duration = Duration.ofDays(30),
    clock: Clock = Clock.systemUTC(),
) : DocumentStore {

    private val attributeMapper: AttributeMapper = AttributeMapper(expiration, clock)

    //region updateDocuments

    override suspend fun updateDocuments(updatedDocuments: Iterable<Document>, checkedDocuments: Iterable<Document>) {
        val updatedList: List<Document> = updatedDocuments.toList()
        val checkedList: List<Document> = checkedDocuments.toList()

        when {
            updatedList.isEmpty() && checkedList.isEmpty() -> {}
            updatedList.size == 1 && checkedList.isEmpty() -> updateSingleDocument(updatedList[0])
            else -> updateMultipleDocuments(updatedList, checkedList)
        }
    }

    private suspend fun updateSingleDocument(document: Document) {
        try {
            client.putItem {
                tableName = this@DynamoDbDocumentStore.tableName
                item = attributeMapper.fromDocument(document)
                conditionExpression = conditionExpression(document.version)
                expressionAttributeValues = expressionAttributeValues(document.version)
            }
        } catch (_: ConditionalCheckFailedException) {
            throw UpdateConflictException(document.id)
        }
    }

    private suspend fun updateMultipleDocuments(updatedDocuments: List<Document>, checkedDocuments: List<Document>) {
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

    private fun conditionExpression(version: Long) =
        if (version == 0L) {
            "attribute_not_exists($PARTITION_KEY)"
        } else {
            "$VERSION = :version"
        }

    private fun expressionAttributeValues(version: Long) =
        if (version == 0L) {
            null
        } else {
            mapOf(":version" to AttributeValue.N(version.toString()))
        }

    //endregion

    //region getDocuments

    override fun getDocuments(ids: Iterable<DocumentKey>): Flow<Document> {
        val idList: List<DocumentKey> = ids.toList()

        return if (idList.isEmpty()) {
            emptyFlow()
        } else if (idList.size == 1) {
            getSingleDocument(idList[0])
        } else {
            getMultipleDocuments(idList)
        }
    }

    private fun getSingleDocument(id: DocumentKey): Flow<Document> = flow {
        val getItemResponse = client.getItem {
            tableName = this@DynamoDbDocumentStore.tableName
            key = attributeMapper.fromDocumentKey(id)
            consistentRead = consistentReads
        }
        val response = getItemResponse.item
        if (response == null) {
            emit(Document(id, null, 0))
        } else {
            emit(attributeMapper.toDocument(response))
        }
    }

    private fun getMultipleDocuments(idList: List<DocumentKey>): Flow<Document> = flow {
        val batchResponse = client.batchGetItem {
            requestItems = mapOf(
                tableName to KeysAndAttributes {
                    keys = idList.distinct().map(attributeMapper::fromDocumentKey)
                    consistentRead = consistentReads
                },
            )
        }

        val responses = batchResponse.responses
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

    //endregion

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
                },
            )
            attributeDefinitions = listOf(
                AttributeDefinition {
                    attributeName = PARTITION_KEY
                    attributeType = ScalarAttributeType.S
                },
                AttributeDefinition {
                    attributeName = SORT_KEY
                    attributeType = ScalarAttributeType.S
                },
            )
            billingMode = BillingMode.PayPerRequest
            configure()
        }
    }
}
