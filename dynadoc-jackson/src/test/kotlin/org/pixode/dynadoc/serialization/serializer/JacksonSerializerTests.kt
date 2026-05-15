package org.pixode.dynadoc.serialization.serializer

import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import java.io.IOException
import java.util.stream.Stream
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlin.test.assertNotNull
import kotlinx.serialization.json.Json.Default.parseToJsonElement
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.parseDocument
import org.pixode.dynadoc.serialization.JsonEntity
import org.pixode.dynadoc.serialization.fromDocument
import org.pixode.dynadoc.serialization.serializer.JacksonSerializerTests.MethodSources.PREFIX
import org.pixode.dynadoc.serialization.toDocument

class JacksonSerializerTests {
    @ParameterizedTest
    @MethodSource("$PREFIX#jsonValid")
    fun serialize_valid(json: String, type: KType, value: Any) {
        val result: String = JacksonJsonSerializer.serializeToString(value)

        assertJsonEquals(json, result)
    }

    @Test
    fun serialize_unknownField() {
        val json = """ { "key": "test", "a": 3, "b": "value" } """
        val value: JsonUnknownFields = JacksonJsonSerializer.deserializeFromString(json, typeOf<JsonUnknownFields>())

        val result: String = JacksonJsonSerializer.serializeToString(value)

        assertJsonEquals(json, result)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#jsonValid")
    fun deserialize_valid(json: String, type: KType, value: Any) {
        val result: Any = JacksonJsonSerializer.deserializeFromString(json, type)

        assertEquals(value, result)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            """ { } """,
            """ { "key": null } """,
        ],
    )
    fun deserialize_null(json: String) {
        val result: JsonStringNullable =
            JacksonJsonSerializer.deserializeFromString(json, typeOf<JsonStringNullable>())

        assertEquals(JsonStringNullable(null), result)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            """ { } """,
            """ { "key": null } """,
        ],
    )
    fun deserialize_default(json: String) {
        val result: JsonStringDefault = JacksonJsonSerializer.deserializeFromString(json, typeOf<JsonStringDefault>())

        assertEquals(JsonStringDefault("default"), result)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#jsonError")
    fun deserialize_error(json: String, type: KType) {
        assertThrows<IOException> {
            JacksonJsonSerializer.deserializeFromString(json, type)
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

        val result: Document = JacksonJsonSerializer.toDocument(document)

        assertJsonEquals(""" { "key": "value" } """, result.body.toString())
    }

    @Test
    fun fromDocument_document() {
        val document = parseDocument(
            id = DocumentKey("PK", "SK"),
            body = """ { "key": "value" } """,
            version = 1,
        )

        val result: JsonEntity<JsonStringValue?> = JacksonJsonSerializer.fromDocument(document)

        assertEquals(
            JsonStringValue("value"),
            result.entity,
        )
    }

    object MethodSources {
        const val PREFIX: String =
            $$"org.pixode.dynadoc.serialization.serializer.JacksonSerializerTests$MethodSources"

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
                    """ { "key": [] } """,
                    typeOf<JsonList>(),
                    JsonList(emptyList()),
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
                    """ { "key": { } } """,
                    typeOf<JsonMap>(),
                    JsonMap(mapOf()),
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
            )
        }
    }

    //region Helper Methods

    private fun assertJsonEquals(expected: String, actual: String?) {
        assertNotNull(actual)
        assertEquals(parseToJsonElement(expected), parseToJsonElement(actual))
    }

    //endregion
}

private data class JsonStringValue(val key: String)

private data class JsonNumberValue(val key: Double)

private data class JsonBooleanValue(val key: Boolean)

private data class JsonList(val key: List<Long>)

private data class JsonMap(val key: Map<String, Long>)

private data class JsonStringNullable(val key: String?)

private data class JsonStringDefault(val key: String = "default")

private data class JsonUnknownFields(
    val key: String,
    @get:JsonAnyGetter
    @param:JsonAnySetter
    val unknownFields: Map<String, Any> = hashMapOf(),
)
