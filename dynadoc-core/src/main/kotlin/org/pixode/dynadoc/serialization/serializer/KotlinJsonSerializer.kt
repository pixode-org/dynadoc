package org.pixode.dynadoc.serialization.serializer

import kotlin.reflect.KType
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.serializer
import org.pixode.dynadoc.serialization.JsonSerializer

class KotlinJsonSerializer(
    private val kotlinJson: Json,
) : JsonSerializer {

    override fun serialize(entity: Any): JsonElement =
        kotlinJson.encodeToJsonElement(
            serializer(entity.javaClass),
            entity,
        )

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any> deserialize(json: JsonElement, type: KType): T =
        kotlinJson.decodeFromJsonElement(serializer(type) as KSerializer<T>, json)
}


val DefaultJsonSerializer = KotlinJsonSerializer(Json)
