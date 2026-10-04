package org.pixode.dynadoc.yugabytedb

import io.r2dbc.spi.Readable
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.parseDocument

private val id: DocumentKey = DocumentKey("PK", "LK")

class RowMapperTests {
    private val rowMapper = RowMapper()

    @Test
    fun fromBody_value() {
        val body: String? = rowMapper.fromBody(parseDocument(id, """ {"a":1.50} """, 4))

        assertEquals(Json.parseToJsonElement(""" {"a":1.50} """), Json.parseToJsonElement(checkNotNull(body)))
    }

    @Test
    fun fromBody_deleted() {
        val body: String? = rowMapper.fromBody(parseDocument(id, null, 4))

        assertNull(body)
    }

    @ParameterizedTest
    @ValueSource(strings = [""" "a" """, """ 10 """, """ true """, """ null """, """ ["a"] """])
    fun fromBody_invalidBody(body: String) {
        assertThrows<IllegalArgumentException> {
            rowMapper.fromBody(parseDocument(id, body, 0))
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
        val document = rowMapper.toDocument(row(rowMapper.fromBody(parseDocument(id, body, 0)), 1))

        assertDocument(document, id, body, 1)
        assertEquals(Json.parseToJsonElement(body).toString(), document.body.toString())
    }

    @Test
    fun toDocument_deleted() {
        val document = rowMapper.toDocument(row(null, 3))

        assertDocument(document, id, null, 3)
    }

    @Test
    fun toDocumentKey_value() {
        assertEquals(id, rowMapper.toDocumentKey(row(null, 3)))
    }

    @Test
    fun toVersion_value() {
        assertEquals(3, rowMapper.toVersion(row(null, 3)))
    }

    private fun row(body: String?, version: Long): Readable = TestRow(
        mapOf(
            PARTITION_KEY to id.partitionKey,
            LOCAL_KEY to id.localKey,
            VERSION to version,
            BODY to body,
        )
    )

    private class TestRow(private val values: Map<String, Any?>) : Readable {
        override fun <T : Any?> get(index: Int, type: Class<T>): T? = throw UnsupportedOperationException()

        override fun <T : Any?> get(name: String, type: Class<T>): T? = type.cast(values.getValue(name))
    }
}
