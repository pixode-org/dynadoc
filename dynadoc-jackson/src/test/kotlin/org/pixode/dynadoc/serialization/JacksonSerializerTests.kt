package org.pixode.dynadoc.serialization

import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.serialization.JacksonSerializerTests.MethodSources.PREFIX
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.skyscreamer.jsonassert.JSONAssert
import java.io.IOException
import java.util.stream.Stream
import kotlin.reflect.KType
import kotlin.reflect.typeOf

class JacksonSerializerTests {
    @ParameterizedTest
    @MethodSource("$PREFIX#jsonValid")
    fun serialize_valid(json: String, type: KType, value: Any) {
        val result: String = DefaultJsonSerializer.serialize(value)

        JSONAssert.assertEquals(json, result, true)
    }

    @Test
    fun serialize_unknownField() {
        val json = """ { "key": "test", "a": 3, "b": "value" } """
        val value: JsonUnknownFields = DefaultJsonSerializer.deserialize(json, typeOf<JsonUnknownFields>())

        val result: String = DefaultJsonSerializer.serialize(value)

        JSONAssert.assertEquals(json, result, true)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#jsonValid")
    fun deserialize_valid(json: String, type: KType, value: Any) {
        val result: Any = DefaultJsonSerializer.deserialize(json, type)

        assertEquals(value, result)
    }

    @ParameterizedTest
    @ValueSource(strings = [
        """ { } """,
        """ { "key": null } """,
    ])
    fun deserialize_null(json: String) {
        val result: JsonStringNullable = DefaultJsonSerializer.deserialize(json, typeOf<JsonStringNullable>())

        assertEquals(JsonStringNullable(null), result)
    }

    @ParameterizedTest
    @ValueSource(strings = [
        """ { } """,
        """ { "key": null } """,
    ])
    fun deserialize_default(json: String) {
        val result: JsonStringDefault = DefaultJsonSerializer.deserialize(json, typeOf<JsonStringDefault>())

        assertEquals(JsonStringDefault("default"), result)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#jsonError")
    fun deserialize_error(json: String, type: KType) {
        assertThrows<IOException> {
            DefaultJsonSerializer.deserialize(json, type)
        }
    }

    @Test
    fun toDocument_document() {
        val document: JsonEntity<JsonStringValue> = JsonEntity(
            id = DocumentKey("PK", "SK"),
            entity = JsonStringValue("value"),
            version = 1,
        )

        val result: Document = DefaultJsonSerializer.toDocument(document)

        JSONAssert.assertEquals(""" { "key": "value" } """, result.body, true)
    }

    @Test
    fun fromDocument_document() {
        val document = Document(
            id = DocumentKey("PK", "SK"),
            body = """ { "key": "value" } """,
            version = 1,
        )

        val result: JsonEntity<JsonStringValue?> = DefaultJsonSerializer.fromDocument(document)

        assertEquals(
            JsonStringValue("value"),
            result.entity)
    }

    object MethodSources {
        const val PREFIX: String = "org.pixode.dynadoc.serialization.JacksonSerializerTests\$MethodSources"

        @JvmStatic
        fun jsonValid(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(
                    """ { "key": "value" } """,
                    typeOf<JsonStringValue>(),
                    JsonStringValue("value"),
                ),
                Arguments.of(
                    """ { "key": 1234567890.12345 } """,
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
                    JsonMap(mapOf(
                        "a" to 1,
                        "b" to 2,
                    )),
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
    @get: JsonAnyGetter
    @param: JsonAnySetter
    val unknownFields: Map<String, Any> = hashMapOf(),
)
