package org.pixode.dynadoc.mongodb

import com.mongodb.MongoException
import com.mongodb.client.model.Filters
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.Updates
import com.mongodb.client.result.UpdateResult
import com.mongodb.kotlin.client.coroutine.ClientSession
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
import org.bson.BsonInt32
import org.bson.BsonString
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
 * Documents are stored with an `_id` of the form `{ partition_key, clustering_key }`, and a `_version` field.
 * Updating multiple documents atomically relies on MongoDB transactions, which require a replica set or a sharded
 * cluster.
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

    //region updateDocuments

    override suspend fun updateDocuments(updatedDocuments: Iterable<Document>, checkedDocuments: Iterable<Document>) {
        val updatedList: List<Document> = updatedDocuments.toList()
        val checkedList: List<Document> = checkedDocuments.toList()

        when {
            updatedList.isEmpty() && checkedList.isEmpty() -> {}
            updatedList.size == 1 && checkedList.isEmpty() -> updateSingleDocument(null, updatedList[0])
            else -> updateMultipleDocuments(updatedList, checkedList)
        }
    }

    private suspend fun updateSingleDocument(session: ClientSession?, document: Document) {
        val bson: BsonDocument = bsonMapper.fromDocument(document)

        detectConflict(document.id) {
            if (document.version == 0L) {
                if (session == null) {
                    collection.insertOne(bson)
                } else {
                    collection.insertOne(session, bson)
                }
            } else {
                val filter: Bson = versionFilter(document.id, document.version)
                val result: UpdateResult = if (session == null) {
                    collection.replaceOne(filter, bson)
                } else {
                    collection.replaceOne(session, filter, bson)
                }

                if (result.matchedCount == 0L) {
                    throw UpdateConflictException(document.id)
                }
            }
        }
    }

    private suspend fun updateMultipleDocuments(updatedDocuments: List<Document>, checkedDocuments: List<Document>) {
        // Validate every document before starting the transaction
        updatedDocuments.forEach(bsonMapper::fromDocument)

        client.startSession().use { session ->
            session.startTransaction()

            try {
                for (document in updatedDocuments) {
                    updateSingleDocument(session, document)
                }

                for (document in checkedDocuments) {
                    checkDocument(session, document)
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

    private suspend fun checkDocument(session: ClientSession, document: Document) {
        detectConflict(document.id) {
            if (document.version == 0L) {
                // Insert then delete a placeholder, so that a concurrent insert of the same document causes a conflict
                val key: BsonDocument = bsonMapper.fromDocumentKey(document.id)
                collection.insertOne(session, BsonDocument(ID, key))
                collection.deleteOne(session, keyFilter(document.id))
            } else {
                // Write the document then revert the change, so that a concurrent update causes a conflict
                val result = collection.updateOne(
                    session,
                    versionFilter(document.id, document.version),
                    Updates.inc(VERSION, 1L),
                )

                if (result.matchedCount == 0L) {
                    throw UpdateConflictException(document.id)
                }

                collection.updateOne(session, keyFilter(document.id), Updates.inc(VERSION, -1L))
            }
        }
    }

    private inline fun <T> detectConflict(id: DocumentKey, action: () -> T): T =
        try {
            action()
        } catch (exception: MongoException) {
            if (exception.code == DUPLICATE_KEY_ERROR || exception.code == WRITE_CONFLICT_ERROR) {
                throw UpdateConflictException(id)
            } else {
                throw exception
            }
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
        database.createCollection(collectionName)
        collection.createIndex(Indexes.ascending("$ID.$PARTITION_KEY", "$ID.$CLUSTERING_KEY"))
        collection.createIndex(Indexes.ascending(DELETED), IndexOptions().expireAfter(0, TimeUnit.SECONDS))
    }

    /**
     * Shards the collection using the partition key as the shard key. This must be executed against a sharded
     * cluster, after [createCollection].
     */
    suspend fun shardCollection() {
        client.getDatabase("admin").runCommand(
            BsonDocument("shardCollection", BsonString("$databaseName.$collectionName"))
                .append("key", BsonDocument("$ID.$PARTITION_KEY", BsonInt32(1))),
        )
    }

    private fun keyFilter(id: DocumentKey): Bson = Filters.and(
        Filters.eq(ID, bsonMapper.fromDocumentKey(id)),
        Filters.eq("$ID.$PARTITION_KEY", id.partitionKey),
    )

    private fun versionFilter(id: DocumentKey, version: Long): Bson = Filters.and(
        keyFilter(id),
        Filters.eq(VERSION, version),
    )
}
