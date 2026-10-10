package org.pixode.dynadoc.serialization

import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toList
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.SortDirection
import org.pixode.dynadoc.core.SortKeyRange
import org.pixode.dynadoc.core.UpdateConflictException

/**
 * Represents a service object used to retrieve and modify documents represented as [JsonEntity] objects.
 */
class EntityStore(
    private val documentStore: DocumentStore,
    private val jsonSerializer: JsonSerializer,
) {
    /**
     * Updates atomically the body of multiple documents represented as [JsonEntity] objects.
     */
    @Throws(UpdateConflictException::class)
    suspend fun updateEntities(
        updatedDocuments: Iterable<JsonEntity<Any?>> = emptyList(),
        checkedDocuments: Iterable<JsonEntity<Any?>> = emptyList(),
    ) {
        documentStore.updateDocuments(
            updatedDocuments = updatedDocuments
                .map(jsonSerializer::toDocument),
            checkedDocuments = checkedDocuments
                .map { entity -> entity.copy(entity = null) }
                .map(jsonSerializer::toDocument),
        )
    }

    /**
     * Retrieves multiple documents of the same type given their IDs, represented as [JsonEntity] objects.
     */
    suspend fun <T : Any> getEntities(ids: Iterable<DocumentKey>, type: KType): List<JsonEntity<T?>> {
        val result = documentStore.getDocuments(ids).toList()
        return result.map { jsonSerializer.fromDocument(it, type) }
    }

    /**
     * Retrieves multiple documents of different types given their IDs, represented as [JsonEntity] objects.
     */
    suspend fun getEntities(ids: List<Pair<DocumentKey, KType>>): List<JsonEntity<Any?>> {
        val result = documentStore.getDocuments(ids.map { it.first }).toList()
        return result.mapIndexed { index, document ->
            jsonSerializer.fromDocument(document, ids[index].second)
        }
    }

    /**
     * Retrieves the documents in a partition, sorted by sort key, whose sort key is in the given range.
     */
    fun <T : Any> getRange(
        type: KType,
        partitionKey: String,
        sortKeyRange: SortKeyRange = SortKeyRange.UNBOUNDED,
        direction: SortDirection = SortDirection.ASCENDING,
    ): Flow<JsonEntity<T>> = documentStore
        .getRange(partitionKey, sortKeyRange, direction)
        .mapNotNull { jsonSerializer.fromDocument<T>(it, type).ifExists() }
}


suspend inline fun <reified T : Any> EntityStore.getEntities(ids: Iterable<DocumentKey>): List<JsonEntity<T?>> =
    getEntities(ids, typeOf<T>())

suspend inline fun <reified T : Any> EntityStore.getEntity(id: DocumentKey): JsonEntity<T?> =
    getEntities<T>(listOf(id), typeOf<T>())[0]

suspend fun EntityStore.updateEntities(vararg updatedEntities: JsonEntity<Any?>) =
    updateEntities(updatedDocuments = updatedEntities.asIterable())

inline fun <reified T : Any> EntityStore.getRange(
    partitionKey: String,
    sortKeyRange: SortKeyRange,
    direction: SortDirection = SortDirection.ASCENDING
): Flow<JsonEntity<T>> = getRange(typeOf<T>(), partitionKey, sortKeyRange, direction)

inline fun <reified T : Any> EntityStore.getPrefixRange(
    partitionKey: String,
    sortKeyPrefix: String,
    direction: SortDirection = SortDirection.ASCENDING
): Flow<JsonEntity<T>> = getRange(partitionKey, SortKeyRange.fromPrefix(sortKeyPrefix), direction)
