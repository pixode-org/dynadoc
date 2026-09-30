package org.pixode.dynadoc.mongodb

import com.mongodb.ClientBulkWriteException
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.WriteError
import com.mongodb.client.model.Filters
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.Updates
import com.mongodb.client.model.bulk.ClientBulkWriteOptions
import com.mongodb.client.model.bulk.ClientBulkWriteResult
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel
import com.mongodb.client.model.bulk.ClientUpdateResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.kotlin.client.coroutine.FindFlow
import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoCollection
import com.mongodb.kotlin.client.coroutine.MongoDatabase
import java.time.Clock
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.bson.BsonDocument
import org.bson.conversions.Bson
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.UpdateConflictException

private const val DUPLICATE_KEY_ERROR = 11000
private const val WRITE_CONFLICT_ERROR = 112

/**
 * Represents an implementation of the [DocumentStore] interface that relies on MongoDB for persistence.
 *
 * Documents are stored with an `_id` of the form `{ partition_key, local_key }`, and a `_version` field.
 * Updating multiple documents atomically relies on MongoDB transactions, which require a replica set or a sharded
 * cluster, and on client bulk writes, which require MongoDB 8.0 or later.
 */
class MongoDbDocumentStore(
    private val client: MongoClient,
    private val databaseName: String,
    private val collectionName: String,
    expiration: Duration = Duration.ofDays(30),
    clock: Clock = Clock.systemUTC(),
) : DocumentStore {

    private val bsonMapper: BsonMapper = BsonMapper(expiration, clock)
    private val database: MongoDatabase = client.getDatabase(databaseName)
    private val collection: MongoCollection<BsonDocument> =
        database.getCollection(collectionName, BsonDocument::class.java)
    private val namespace: MongoNamespace = MongoNamespace(databaseName, collectionName)
    private val bulkWriteOptions: ClientBulkWriteOptions = ClientBulkWriteOptions.clientBulkWriteOptions()
        .ordered(true)
        .verboseResults(true)

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
        val bson: BsonDocument = bsonMapper.fromDocument(document)

        detectConflict(document.id) {
            if (document.version == 0L) {
                collection.insertOne(bson)
            } else {
                val result: UpdateResult = collection.replaceOne(versionFilter(document.id, document.version), bson)

                if (result.matchedCount == 0L) {
                    throw UpdateConflictException(document.id)
                }
            }
        }
    }

    private suspend fun updateMultipleDocuments(updatedDocuments: List<Document>, checkedDocuments: List<Document>) {
        val operations: List<WriteOperation> =
            updatedDocuments.map(::updateOperation) + checkedDocuments.flatMap(::checkOperations)

        client.startSession().use { session ->
            session.startTransaction()

            try {
                // Send all the operations in a single round trip
                val result: ClientBulkWriteResult = detectBulkConflict(operations) {
                    client.bulkWrite(session, operations.map { it.model }, bulkWriteOptions)
                }

                val updateResults: Map<Int, ClientUpdateResult> = result.verboseResults.get().updateResults
                val conflict: WriteOperation? = operations.withIndex()
                    .firstOrNull { (index, operation) ->
                        operation.mustMatch && updateResults.getValue(index).matchedCount == 0L
                    }
                    ?.value

                if (conflict != null) {
                    throw UpdateConflictException(conflict.id)
                }

                session.commitTransaction()
            } catch (exception: Throwable) {
                if (session.hasActiveTransaction()) {
                    session.abortTransaction()
                }
                throw exception
            }
        }
    }

    private fun updateOperation(document: Document): WriteOperation {
        val bson: BsonDocument = bsonMapper.fromDocument(document)

        return if (document.version == 0L) {
            WriteOperation(document.id, ClientNamespacedWriteModel.insertOne(namespace, bson))
        } else {
            WriteOperation(
                id = document.id,
                model = ClientNamespacedWriteModel.replaceOne(
                    namespace,
                    versionFilter(document.id, document.version),
                    bson,
                ),
                mustMatch = true,
            )
        }
    }

    private fun checkOperations(document: Document): List<WriteOperation> =
        if (document.version == 0L) {
            // Insert then delete a placeholder, so that a concurrent insert of the same document causes a conflict
            val key: BsonDocument = bsonMapper.fromDocumentKey(document.id)
            listOf(
                WriteOperation(document.id, ClientNamespacedWriteModel.insertOne(namespace, BsonDocument(ID, key))),
                WriteOperation(document.id, ClientNamespacedWriteModel.deleteOne(namespace, keyFilter(document.id))),
            )
        } else {
            // Write the document then revert the change, so that a concurrent update causes a conflict.
            listOf(
                WriteOperation(
                    id = document.id,
                    model = ClientNamespacedWriteModel.updateOne(
                        namespace,
                        versionFilter(document.id, document.version),
                        Updates.inc(VERSION, 1L),
                    ),
                    mustMatch = true,
                ),
                WriteOperation(
                    id = document.id,
                    model = ClientNamespacedWriteModel.updateOne(
                        namespace,
                        versionFilter(document.id, document.version + 1),
                        Updates.inc(VERSION, -1L),
                    ),
                ),
            )
        }

    private inline fun <T> detectConflict(id: DocumentKey, action: () -> T): T =
        try {
            action()
        } catch (exception: MongoException) {
            if (isConflict(exception.code)) {
                throw UpdateConflictException(id)
            } else {
                throw exception
            }
        }

    private inline fun <T> detectBulkConflict(operations: List<WriteOperation>, action: () -> T): T =
        try {
            action()
        } catch (exception: ClientBulkWriteException) {
            val writeError: Map.Entry<Int, WriteError>? =
                exception.writeErrors.entries.firstOrNull { isConflict(it.value.code) }

            if (writeError != null) {
                throw UpdateConflictException(operations[writeError.key].id)
            } else {
                throw exception
            }
        } catch (exception: MongoException) {
            if (isConflict(exception.code)) {
                throw UpdateConflictException(operations[0].id)
            } else {
                throw exception
            }
        }

    private fun isConflict(code: Int): Boolean =
        code == DUPLICATE_KEY_ERROR || code == WRITE_CONFLICT_ERROR

    private class WriteOperation(
        val id: DocumentKey,
        val model: ClientNamespacedWriteModel,
        val mustMatch: Boolean = false,
    )

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
        val response: BsonDocument? = collection.find(keyFilter(id)).firstOrNull()

        if (response == null) {
            emit(Document(id, null, 0))
        } else {
            emit(bsonMapper.toDocument(response))
        }
    }

    private fun getMultipleDocuments(idList: List<DocumentKey>): Flow<Document> = flow {
        val distinctIds: List<DocumentKey> = idList.distinct()
        val filter: Bson = Filters.and(
            Filters.`in`(ID, distinctIds.map(bsonMapper::fromDocumentKey)),
            Filters.`in`("$ID.$PARTITION_KEY", distinctIds.map { it.partitionKey }.distinct()),
        )

        val documents: Map<DocumentKey, Document> = collection.find(filter)
            .toList()
            .map(bsonMapper::toDocument)
            .associateBy { it.id }

        val result = idList.map { id ->
            documents[id] ?: Document(id, null, 0)
        }

        result.asFlow().collect(this)
    }

    //endregion

    /**
     * Finds the documents matching the given filter.
     */
    fun find(filter: Bson, configure: FindFlow<BsonDocument>.() -> Unit = { }): Flow<Document> =
        collection.find(filter)
            .apply(configure)
            .map(bsonMapper::toDocument)

    /**
     * Creates the collection, along with an index on the document key and a TTL index on deleted documents.
     */
    suspend fun createCollection() {
        collection.createIndex(Indexes.ascending(DELETED), IndexOptions().expireAfter(0, TimeUnit.SECONDS))
    }

    // An equality on the whole _id is enough for the query to be routed to a single shard
    private fun keyFilter(id: DocumentKey): Bson = Filters.eq(ID, bsonMapper.fromDocumentKey(id))

    private fun versionFilter(id: DocumentKey, version: Long): Bson = Filters.and(
        keyFilter(id),
        Filters.eq(VERSION, version),
    )
}
