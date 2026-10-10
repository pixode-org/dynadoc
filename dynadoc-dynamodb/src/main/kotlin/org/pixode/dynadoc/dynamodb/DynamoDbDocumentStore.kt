package org.pixode.dynadoc.dynamodb

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
import aws.sdk.kotlin.services.dynamodb.transactWriteItems
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flow
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.SortDirection
import org.pixode.dynadoc.core.SortKeyBound
import org.pixode.dynadoc.core.SortKeyRange
import org.pixode.dynadoc.core.UpdateConflictException

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
        val start: SortKeyBound = sortKeyRange.start
        val end: SortKeyBound = sortKeyRange.end

        val values = mutableMapOf<String, AttributeValue>(":partitionKey" to AttributeValue.S(partitionKey))

        // A key condition can only hold a single condition on the sort key, so when both bounds are specified, the
        // key condition is inclusive and the excluded bounds are removed from the result, since a filter expression
        // can't refer to a key attribute
        val sortKeyCondition: String? = when {
            start !is SortKeyBound.Unbounded && end !is SortKeyBound.Unbounded -> "#sortKey BETWEEN :start AND :end"
            start is SortKeyBound.Inclusive -> "#sortKey >= :start"
            start is SortKeyBound.Exclusive -> "#sortKey > :start"
            end is SortKeyBound.Inclusive -> "#sortKey <= :end"
            end is SortKeyBound.Exclusive -> "#sortKey < :end"
            else -> null
        }

        start.valueOrNull()?.let { values[":start"] = AttributeValue.S(it) }
        end.valueOrNull()?.let { values[":end"] = AttributeValue.S(it) }

        val names = mutableMapOf(
            "#partitionKey" to PARTITION_KEY,
            "#deleted" to DELETED,
        )
        if (sortKeyCondition != null) {
            names["#sortKey"] = SORT_KEY
        }

        val excludedSortKeys: Set<String> = setOfNotNull(
            (start as? SortKeyBound.Exclusive)?.value,
            (end as? SortKeyBound.Exclusive)?.value,
        )

        return query {
            keyConditionExpression = listOfNotNull("#partitionKey = :partitionKey", sortKeyCondition)
                .joinToString(" AND ")
            filterExpression = "attribute_not_exists(#deleted)"
            expressionAttributeNames = names
            expressionAttributeValues = values
            consistentRead = consistentReads
            scanIndexForward = direction == SortDirection.ASCENDING
        }.filter { it.id.sortKey !in excludedSortKeys }
    }

    private fun SortKeyBound.valueOrNull(): String? = when (this) {
        is SortKeyBound.Inclusive -> value
        is SortKeyBound.Exclusive -> value
        SortKeyBound.Unbounded -> null
    }

    //endregion

    fun query(queryRequest: QueryRequest.Builder.() -> Unit): Flow<Document> = flow {
        val request = QueryRequest {
            tableName = this@DynamoDbDocumentStore.tableName
            queryRequest()
        }

        var startKey: Map<String, AttributeValue>? = null
        do {
            val request = request.copy {
                if (startKey != null) {
                    exclusiveStartKey = startKey
                }
            }
            val response = client.query(request)
            for (item in response.items ?: emptyList()) {
                emit(attributeMapper.toDocument(item))
            }

            startKey = response.lastEvaluatedKey?.takeIf { it.isNotEmpty() }
        } while (startKey != null)
    }

    fun scan(scanRequest: ScanRequest.Builder.() -> Unit): Flow<Document> = flow {
        val request = ScanRequest {
            tableName = this@DynamoDbDocumentStore.tableName
            scanRequest()
        }

        var startKey: Map<String, AttributeValue>? = null
        do {
            val request = request.copy {
                if (startKey != null) {
                    exclusiveStartKey = startKey
                }
            }
            val response = client.scan(request)
            for (item in response.items ?: emptyList()) {
                emit(attributeMapper.toDocument(item))
            }

            startKey = response.lastEvaluatedKey?.takeIf { it.isNotEmpty() }
        } while (startKey != null)
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
