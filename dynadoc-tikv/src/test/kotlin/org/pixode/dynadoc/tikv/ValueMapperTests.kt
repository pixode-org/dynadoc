package org.pixode.dynadoc.tikv

import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.parseDocument

private val id: DocumentKey = DocumentKey("PK", "LK")

class ValueMapperTests {
    private val valueMapper = ValueMapper()

    @Test
    fun fromDocument_value() {
        val value: ByteArray = valueMapper.fromDocument(parseDocument(id, """ {"a":1.50} """, 4))

        assertEquals(
            Json.parseToJsonElement(""" {"partition_key":"PK","local_key":"LK","version":5,"body":{"a":1.50}} """),
            Json.parseToJsonElement(value.toString(Charsets.UTF_8)),
        )
    }

    @Test
    fun fromDocument_deleted() {
        val value: ByteArray = valueMapper.fromDocument(parseDocument(id, null, 4))

        assertEquals(
            Json.parseToJsonElement(""" {"partition_key":"PK","local_key":"LK","version":5,"body":null} """),
            Json.parseToJsonElement(value.toString(Charsets.UTF_8)),
        )
    }

    @ParameterizedTest
    @ValueSource(strings = [""" "a" """, """ 10 """, """ true """, """ null """, """ ["a"] """])
    fun fromDocument_invalidBody(body: String) {
        assertThrows<IllegalArgumentException> {
            valueMapper.fromDocument(parseDocument(id, body, 0))
        }
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            """ {"a":"b"} """,
            """ {"partition_key":"x","local_key":"y","version":10,"body":null} """,
            """ {"n":1234567890.0987654321,"m":99999999999999999999,"e":1e3} """,
            """ {"a":{"b":[1,"c",null,true,{}]}} """,
        ],
    )
    fun toDocument_roundTrip(body: String) {
        val document = valueMapper.toDocument(valueMapper.fromDocument(parseDocument(id, body, 0)))

        assertDocument(document, id, body, 1)
        assertEquals(Json.parseToJsonElement(body).toString(), document.body.toString())
    }

    @Test
    fun toDocument_deleted() {
        val document = valueMapper.toDocument(valueMapper.fromDocument(parseDocument(id, null, 2)))

        assertDocument(document, id, null, 3)
    }
}
