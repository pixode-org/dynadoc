package org.dynadoc.core

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant

const val PARTITION_KEY = "partition_key"
const val SORT_KEY = "sort_key"
const val VERSION = "version"
const val DELETED = "deleted"

val systemAttributes: Set<String> = setOf(PARTITION_KEY, SORT_KEY, VERSION, DELETED)

class AttributeMapper(
    private val expiration: Duration,
    private val clock: Clock
) {
    fun toDocument(attributes: Map<String, AttributeValue>): Document {
        val body: String? =
            if (attributes[DELETED] != null) {
                null
            } else {
                val bodyMap: Map<String, AttributeValue> = attributes.filterKeys { it !in systemAttributes }
                Json.encodeToString(bodyMap.mapValues { (_, v) -> attributeValueToJson(v) })
            }

        return Document(
            id = toDocumentKey(attributes),
            body = body,
            version = (attributes.getValue(VERSION) as AttributeValue.N).value.toLong()
        )
    }

    fun fromDocument(document: Document): Map<String, AttributeValue> = buildMap {
        if (document.body != null) {
            require(jsonObject.containsMatchIn(document.body)) {
                "The document must be a valid JSON object."
            }

            val attributes: Map<String, AttributeValue> = try {
                jsonToAttributeValueMap(document.body)
            } catch (e: SerializationException) {
                throw IllegalArgumentException("The document must be a valid JSON object.", e)
            }

            val specialAttribute: String? = systemAttributes.firstOrNull { key -> attributes.containsKey(key) }
            require(specialAttribute == null) {
                "The document cannot use the special attribute \"$specialAttribute\"."
            }

            putAll(attributes)
        }

        putAll(fromDocumentKey(document.id))
        put(VERSION, AttributeValue.N((document.version + 1).toString()))

        if (document.body == null) {
            val expiration: Instant = clock.instant() + expiration
            put(DELETED, AttributeValue.N(expiration.epochSecond.toString()))
        }
    }

    fun fromDocumentKey(id: DocumentKey) = mapOf(
        PARTITION_KEY to AttributeValue.S(id.partitionKey),
        SORT_KEY to AttributeValue.S(id.sortKey)
    )

    fun toDocumentKey(attributes: Map<String, AttributeValue>): DocumentKey = DocumentKey(
        partitionKey = (attributes.getValue(PARTITION_KEY) as AttributeValue.S).value,
        sortKey = (attributes.getValue(SORT_KEY) as AttributeValue.S).value
    )

    private fun jsonToAttributeValueMap(json: String): Map<String, AttributeValue> =
        (Json.parseToJsonElement(json) as JsonObject).mapValues { (_, v) -> jsonElementToAttributeValue(v) }

    private fun jsonElementToAttributeValue(element: JsonElement): AttributeValue = when (element) {
        is JsonNull -> AttributeValue.Null(true)
        is JsonPrimitive if element.isString -> AttributeValue.S(element.content)
        is JsonPrimitive if element.booleanOrNull != null -> AttributeValue.Bool(element.boolean)
        is JsonPrimitive -> AttributeValue.N(element.content)
        is JsonArray -> AttributeValue.L(element.map { jsonElementToAttributeValue(it) })
        is JsonObject -> AttributeValue.M(element.mapValues { (_, v) -> jsonElementToAttributeValue(v) })
    }

    private fun attributeValueToJson(value: AttributeValue): JsonElement = when (value) {
        is AttributeValue.S -> JsonPrimitive(value.value)
        is AttributeValue.N -> JsonPrimitive(BigDecimal(value.value))
        is AttributeValue.Bool -> JsonPrimitive(value.value)
        is AttributeValue.Null -> JsonNull
        is AttributeValue.L -> JsonArray(value.value.map(::attributeValueToJson))
        is AttributeValue.M -> JsonObject(value.value.mapValues { (_, v) -> attributeValueToJson(v) })
        else -> throw UnsupportedOperationException("Unsupported attribute type: ${value.javaClass.simpleName}")
    }

    private companion object {
        private val jsonObject: Regex = Regex("^\\s*\\{")
    }
}
