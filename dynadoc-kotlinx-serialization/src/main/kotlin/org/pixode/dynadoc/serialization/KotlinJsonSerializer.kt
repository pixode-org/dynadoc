package org.pixode.dynadoc.serialization

import kotlin.reflect.KType
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer

class KotlinJsonSerializer(
    private val kotlinJson: Json,
) : JsonSerializer {

    override fun serialize(entity: Any): String =
        kotlinJson.encodeToString(
            serializer(entity.javaClass),
            entity,
        )

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any> deserialize(json: String, type: KType): T =
        kotlinJson.decodeFromString(serializer(type) as KSerializer<T>, json)
}


val DefaultJsonSerializer = KotlinJsonSerializer(Json)
