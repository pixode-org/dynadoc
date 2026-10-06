package org.pixode.dynadoc.tidb

import io.r2dbc.spi.Readable
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.pixode.dynadoc.core.DocumentKey

private val id: DocumentKey = DocumentKey("PK", "SK")

class RowMapperTests {
    @ParameterizedTest
    @ValueSource(
        strings = [
            """ {"a":"b"} """,
            """ {"partition_key":"x","sort_key":"y","version":10,"body":null} """,
            """ {"n":1234567890.0987654321,"m":99999999999999999999,"e":1e3} """,
            """ {"a":{"b":[1,"c",null,true,{}]}} """,
        ],
    )
    fun toDocument_roundTrip(body: String) {
        val document = RowMapper.toDocument(row(body, 1))

        assertDocument(document, id, body, 1)
        assertEquals(Json.parseToJsonElement(body).toString(), document.body.toString())
    }

    @Test
    fun toDocument_deleted() {
        val document = RowMapper.toDocument(row(null, 3))

        assertDocument(document, id, null, 3)
    }

    @Test
    fun toDocumentKey_value() {
        assertEquals(id, RowMapper.toDocumentKey(row(null, 3)))
    }

    @ParameterizedTest
    @ValueSource(strings = [PARTITION_KEY, SORT_KEY, VERSION])
    fun toDocument_nullColumn(column: String) {
        assertThrows<IllegalStateException> {
            RowMapper.toDocument(row(""" {"a":"b"} """, 1, nullColumn = column))
        }
    }

    private fun row(body: String?, version: Long, nullColumn: String? = null): Readable = TestRow(
        mapOf(
            PARTITION_KEY to id.partitionKey,
            SORT_KEY to id.sortKey,
            VERSION to version,
            BODY to body,
        ) + listOfNotNull(nullColumn).associateWith { null }
    )

    private class TestRow(private val values: Map<String, Any?>) : Readable {
        override fun <T : Any?> get(index: Int, type: Class<T>): T? = throw UnsupportedOperationException()

        override fun <T : Any?> get(name: String, type: Class<T>): T? = type.cast(values.getValue(name))
    }
}
