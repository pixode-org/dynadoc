package org.pixode.dynadoc.core

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

private val nonNullDocument = Document(
    id = DocumentKey("PK", "SK"),
    body = JsonArray(
        listOf(
            JsonObject(mapOf("a" to JsonPrimitive(1))),
            JsonNull,
            JsonPrimitive("value"),
        )
    ),
    version = 2L,
)
private val nullDocument = Document(DocumentKey("PK", "SK"), null, 3L)

class DocumentTests {
    @Test
    fun toString_nonNullBody() {
        assertEquals(
            """Document(id=("PK", "SK"), body=[{"a":1},null,"value"], version=2)""",
            nonNullDocument.toString()
        )
    }

    @Test
    fun toString_nullBody() {
        assertEquals(
            """Document(id=("PK", "SK"), body=null, version=3)""",
            nullDocument.toString()
        )
    }

    @Test
    fun parseDocument_nonNullBody() {
        val result = parseDocument(
            id = DocumentKey("PK", "SK"),
            body = """ [ { "a": 1 }, null, "value" ] """,
            version = 2L,
        )

        assertEquals(nonNullDocument, result)
    }

    @Test
    fun parseDocument_nullBody() {
        val result = parseDocument(
            id = DocumentKey("PK", "SK"),
            body = null,
            version = 3L,
        )

        assertEquals(nullDocument, result)
    }
}
