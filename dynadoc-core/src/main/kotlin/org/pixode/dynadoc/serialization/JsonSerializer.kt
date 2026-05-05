package org.pixode.dynadoc.serialization

import org.pixode.dynadoc.core.Document
import kotlin.reflect.KType
import kotlin.reflect.typeOf

interface JsonSerializer {
    /**
     * Serializes the specified object to a JSON string.
     */
    fun serialize(entity: Any): String

    /**
     * Deserializes a JSON string to the specified type.
     */
    fun <T : Any> deserialize(json: String, type: KType): T
}


fun JsonSerializer.toDocument(jsonEntity: JsonEntity<Any?>): Document =
    Document(
        id = jsonEntity.id,
        body = jsonEntity.entity?.let { serialize(it) },
        version = jsonEntity.version
    )

fun <T : Any> JsonSerializer.fromDocument(document: Document, type: KType): JsonEntity<T?> =
    JsonEntity(
        id = document.id,
        entity = document.body?.let { deserialize(it, type) },
        version = document.version
    )

inline fun <reified T : Any> JsonSerializer.fromDocument(document: Document): JsonEntity<T?> =
    fromDocument(document, typeOf<T>())
