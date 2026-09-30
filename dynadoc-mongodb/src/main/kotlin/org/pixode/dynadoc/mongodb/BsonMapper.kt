package org.pixode.dynadoc.mongodb

import java.math.BigDecimal
import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.booleanOrNull
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDecimal128
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonNull
import org.bson.BsonString
import org.bson.BsonValue
import org.bson.types.Decimal128
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey

const val ID = "_id"
const val PARTITION_KEY = "pk"
const val LOCAL_KEY = "lk"
const val VERSION = "_version"
const val DELETED = "_deleted"

val reservedFields: Set<String> = setOf(ID, VERSION, DELETED)

class BsonMapper(
    private val expiration: Duration,
    private val clock: Clock,
) {
    fun toDocument(bson: BsonDocument): Document {
        val body: JsonObject? =
            if (bson[DELETED] != null) {
                null
            } else {
                val bodyMap: Map<String, BsonValue> = bson.filterKeys { it !in reservedFields }
                JsonObject(bodyMap.mapValues { (_, v) -> bsonValueToJson(v) })
            }

        return Document(
            id = toDocumentKey(bson.getValue(ID).asDocument()),
            body = body,
            version = bson.getValue(VERSION).asNumber().longValue(),
        )
    }

    fun fromDocument(document: Document): BsonDocument {
        val result = BsonDocument()
        result[ID] = fromDocumentKey(document.id)
        result[VERSION] = BsonInt64(document.version + 1)

        val body: JsonElement? = document.body
        if (body != null) {
            require(body is JsonObject) {
                "The document must be a valid JSON object"
            }

            val reservedField: String? = reservedFields.firstOrNull { key -> body.containsKey(key) }
            require(reservedField == null) {
                "The document cannot use the special attribute \"$reservedField\""
            }

            for ((key, value) in body) {
                result[key] = jsonElementToBsonValue(value)
            }
        } else {
            val expiration: Instant = clock.instant() + expiration
            result[DELETED] = BsonDateTime(expiration.toEpochMilli())
        }

        return result
    }

    fun fromDocumentKey(id: DocumentKey): BsonDocument = BsonDocument(
        listOf(
            BsonElement(PARTITION_KEY, BsonString(id.partitionKey)),
            BsonElement(LOCAL_KEY, BsonString(id.localKey)),
        )
    )

    fun toDocumentKey(id: BsonDocument): DocumentKey = DocumentKey(
        partitionKey = id.getValue(PARTITION_KEY).asString().value,
        localKey = id.getValue(LOCAL_KEY).asString().value,
    )

    private fun jsonElementToBsonValue(element: JsonElement): BsonValue = when (element) {
        is JsonNull -> BsonNull.VALUE
        is JsonPrimitive if element.isString -> BsonString(element.content)
        is JsonPrimitive if element.booleanOrNull != null -> BsonBoolean(element.boolean)
        is JsonPrimitive -> BsonDecimal128(Decimal128(BigDecimal(element.content)))
        is JsonArray -> BsonArray(element.map { jsonElementToBsonValue(it) })
        is JsonObject -> BsonDocument().apply {
            for ((key, value) in element) {
                this[key] = jsonElementToBsonValue(value)
            }
        }
    }

    private fun bsonValueToJson(value: BsonValue): JsonElement = when (value) {
        is BsonString -> JsonPrimitive(value.value)
        is BsonInt32 -> JsonPrimitive(value.value)
        is BsonInt64 -> JsonPrimitive(value.value)
        is BsonDouble -> JsonPrimitive(BigDecimal.valueOf(value.value))
        is BsonDecimal128 -> JsonPrimitive(value.value.bigDecimalValue())
        is BsonBoolean -> JsonPrimitive(value.value)
        is BsonNull -> JsonNull
        is BsonArray -> JsonArray(value.values.map(::bsonValueToJson))
        is BsonDocument -> JsonObject(value.mapValues { (_, v) -> bsonValueToJson(v) })
        else -> throw UnsupportedOperationException("Unsupported BSON type: ${value.bsonType}")
    }
}
