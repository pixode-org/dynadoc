package org.pixode.dynadoc.tikv

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey

const val PARTITION_KEY = "partition_key"
const val LOCAL_KEY = "local_key"
const val VERSION = "version"
const val BODY = "body"

/**
 * Maps documents to TiKV values.
 *
 * A value is a UTF-8 encoded JSON object of the form `{ partition_key, local_key, version, body }`. The document key is stored in the
 * value since it can't be recovered from the TiKV key. A deleted document is stored with a null body.
 */
class ValueMapper {
    fun toDocument(value: ByteArray): Document {
        val json: JsonObject = Json.parseToJsonElement(value.toString(Charsets.UTF_8)).jsonObject
        val body: JsonElement = json.getValue(BODY)

        return Document(
            id = DocumentKey(
                partitionKey = json.getValue(PARTITION_KEY).jsonPrimitive.content,
                localKey = json.getValue(LOCAL_KEY).jsonPrimitive.content,
            ),
            body = if (body is JsonNull) null else body,
            version = json.getValue(VERSION).jsonPrimitive.long,
        )
    }

    fun fromDocument(document: Document): ByteArray {
        val body: JsonElement? = document.body
        require(body == null || body is JsonObject) {
            "The document must be a valid JSON object"
        }

        val json = JsonObject(
            mapOf(
                PARTITION_KEY to JsonPrimitive(document.id.partitionKey),
                LOCAL_KEY to JsonPrimitive(document.id.localKey),
                VERSION to JsonPrimitive(document.version + 1),
                BODY to (body ?: JsonNull),
            )
        )

        return json.toString().toByteArray(Charsets.UTF_8)
    }
}
