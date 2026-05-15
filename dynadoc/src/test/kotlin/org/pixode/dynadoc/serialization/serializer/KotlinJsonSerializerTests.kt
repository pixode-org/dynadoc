package org.pixode.dynadoc.serialization.serializer

import java.util.stream.Stream
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json.Default.decodeFromString
import kotlinx.serialization.json.Json.Default.parseToJsonElement
import kotlinx.serialization.json.JsonElement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.parseDocument
import org.pixode.dynadoc.serialization.JsonEntity
import org.pixode.dynadoc.serialization.fromDocument
import org.pixode.dynadoc.serialization.serializer.KotlinJsonSerializerTests.MethodSources.PREFIX
import org.pixode.dynadoc.serialization.toDocument

class KotlinJsonSerializerTests {
    @ParameterizedTest
    @MethodSource("$PREFIX#jsonValid")
    fun serialize_valid(json: String, type: KType, value: Any) {
        val result: JsonElement = DefaultJsonSerializer.serialize(value)

        assertJsonEquals(json, result)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#jsonValid")
    fun deserialize_valid(json: String, type: KType, value: Any) {
        val result: Any = DefaultJsonSerializer.deserialize(decodeFromString(json), type)

        assertEquals(value, result)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#jsonError")
    fun deserialize_error(json: String, type: KType) {
        assertThrows<SerializationException> {
            DefaultJsonSerializer.deserialize(decodeFromString(json), type)
        }
    }

    @Test
    fun toDocument_document() {
        val document: JsonEntity<JsonStringValue> =
            JsonEntity(
                id = DocumentKey("PK", "SK"),
                entity = JsonStringValue("value"),
                version = 1,
            )

        val result: Document = DefaultJsonSerializer.toDocument(document)

        assertJsonEquals(""" { "key": "value" } """, result.body)
    }

    @Test
    fun fromDocument_document() {
        val document = parseDocument(
            id = DocumentKey("PK", "SK"),
            body = """ { "key": "value" } """,
            version = 1,
        )

        val result: JsonEntity<JsonStringValue?> = DefaultJsonSerializer.fromDocument(document)

        assertEquals(
            JsonStringValue("value"),
            result.entity,
        )
    }

    object MethodSources {
        const val PREFIX: String =
            $$"org.pixode.dynadoc.serialization.serializer.KotlinJsonSerializerTests$MethodSources"

        @JvmStatic
        fun jsonValid(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(
                    """ { "key": "value" } """,
                    typeOf<JsonStringValue>(),
                    JsonStringValue("value"),
                ),
                Arguments.of(
                    """ { "key": 1.23456789012345E9 } """,
                    typeOf<JsonNumberValue>(),
                    JsonNumberValue(1234567890.12345),
                ),
                Arguments.of(
                    """ { "key": true } """,
                    typeOf<JsonBooleanValue>(),
                    JsonBooleanValue(true),
                ),
                Arguments.of(
                    """ { "key": [10, 20, 30] } """,
                    typeOf<JsonList>(),
                    JsonList(listOf(10, 20, 30)),
                ),
                Arguments.of(
                    """ { "key": { "a": 1, "b": 2 } } """,
                    typeOf<JsonMap>(),
                    JsonMap(
                        mapOf(
                            "a" to 1,
                            "b" to 2,
                        ),
                    ),
                ),
                Arguments.of(
                    """ { "key": "value" } """,
                    typeOf<JsonStringNullable>(),
                    JsonStringNullable("value"),
                ),
                Arguments.of(
                    """ { "key": null } """,
                    typeOf<JsonStringNullable>(),
                    JsonStringNullable(null),
                ),
                Arguments.of(
                    """ { } """,
                    typeOf<JsonStringDefault>(),
                    JsonStringDefault("default"),
                ),
                Arguments.of(
                    """ { } """,
                    typeOf<JsonStringDefaultNullable>(),
                    JsonStringDefaultNullable(null),
                ),
                Arguments.of(
                    """ { "key": "value" } """,
                    typeOf<JsonStringDefault>(),
                    JsonStringDefault("value"),
                ),
            )
        }

        @JvmStatic
        fun jsonError(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(
                    """ { "key": null } """,
                    typeOf<JsonStringValue>(),
                ),
                Arguments.of(
                    """ { } """,
                    typeOf<JsonStringValue>(),
                ),
                Arguments.of(
                    """ { } """,
                    typeOf<JsonStringNullable>(),
                ),
                Arguments.of(
                    """ { "key": null } """,
                    typeOf<JsonStringDefault>(),
                ),
            )
        }
    }

    //region Helper Methods

    private fun assertJsonEquals(expected: String, actual: JsonElement?) =
        assertEquals(parseToJsonElement(expected), actual)

    //endregion
}

@Serializable
private data class JsonStringValue(val key: String)

@Serializable
private data class JsonNumberValue(val key: Double)

@Serializable
private data class JsonBooleanValue(val key: Boolean)

@Serializable
private data class JsonList(val key: List<Long>)

@Serializable
private data class JsonMap(val key: Map<String, Long>)

@Serializable
private data class JsonStringNullable(val key: String?)

@Serializable
private data class JsonStringDefault(val key: String = "default")

@Serializable
private data class JsonStringDefaultNullable(val key: String? = null)
