package org.pixode.dynadoc.serialization.serializer

import kotlin.reflect.KType
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import org.pixode.dynadoc.serialization.JsonSerializer

abstract class StringJsonSerializer : JsonSerializer {
    abstract fun serializeToString(entity: Any): String

    abstract fun <T : Any> deserializeFromString(json: String, type: KType): T

    override fun serialize(entity: Any): JsonElement = Json.parseToJsonElement(serializeToString(entity))

    override fun <T : Any> deserialize(json: JsonElement, type: KType): T = deserializeFromString(json.toString(), type)
}
