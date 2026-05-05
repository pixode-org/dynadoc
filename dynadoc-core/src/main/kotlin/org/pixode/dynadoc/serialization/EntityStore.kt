package org.pixode.dynadoc.serialization

import kotlinx.coroutines.flow.toList
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.UpdateConflictException
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/**
 * Represents a service object used to retrieve and modify documents represented as [JsonEntity] objects.
 */
class EntityStore(
    private val documentStore: DocumentStore,
    private val jsonSerializer: JsonSerializer
) {
    /**
     * Updates atomically the body of multiple documents represented as [JsonEntity] objects.
     */
    @Throws(UpdateConflictException::class)
    suspend fun updateEntities(
        updatedDocuments: Iterable<JsonEntity<Any?>> = emptyList(),
        checkedDocuments: Iterable<JsonEntity<Any?>> = emptyList()
    ) {
        documentStore.updateDocuments(
            updatedDocuments = updatedDocuments
                .map(jsonSerializer::toDocument),
            checkedDocuments = checkedDocuments
                .map { entity -> entity.copy(entity = null) }
                .map(jsonSerializer::toDocument)
        )
    }

    /**
     * Retrieves a document given its ID, represented as a [JsonEntity] object.
     */
    suspend fun <T : Any> getEntities(ids: Iterable<DocumentKey>, type: KType): List<JsonEntity<T?>> {
        val result = documentStore.getDocuments(ids).toList()
        return result.map { jsonSerializer.fromDocument(it, type) }
    }
}


suspend inline fun <reified T : Any> EntityStore.getEntities(ids: Iterable<DocumentKey>): List<JsonEntity<T?>> =
    getEntities(ids, typeOf<T>())

suspend inline fun <reified T : Any> EntityStore.getEntity(id: DocumentKey): JsonEntity<T?> =
    getEntities<T>(listOf(id), typeOf<T>())[0]

suspend fun EntityStore.updateEntities(vararg updatedEntities: JsonEntity<Any?>) =
    updateEntities(updatedDocuments = updatedEntities.asIterable())
