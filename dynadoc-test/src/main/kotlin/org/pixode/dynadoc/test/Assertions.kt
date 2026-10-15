package org.pixode.dynadoc.test

import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey

fun assertDocument(document: Document, id: DocumentKey, body: String?, version: Long) {
    assertEquals(id, document.id)

    if (body == null) {
        assertNull(document.body)
    } else {
        assertNotNull(document.body)
        assertEquals(Json.parseToJsonElement(body), document.body)
    }

    assertEquals(version, document.version)
}

fun assertDocuments(actual: List<Document>, expected: List<Document>) {
    assertEquals(expected.size, actual.size)

    repeat(actual.size) { i ->
        assertDocument(actual[i], expected[i].id, expected[i].body.toString(), 1)
    }
}
