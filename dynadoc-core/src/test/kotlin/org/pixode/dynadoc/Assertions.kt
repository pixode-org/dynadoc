package org.pixode.dynadoc;

import io.mockk.coVerify
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.serialization.JsonEntity
import org.junit.jupiter.api.Assertions.*
import org.skyscreamer.jsonassert.JSONAssert
import kotlin.test.assertContentEquals

fun assertDocument(document: Document, id: DocumentKey, body: String?, version: Long) {
    assertEquals(id, document.id)

    if (body == null) {
        assertNull(document.body)
    } else {
        assertNotNull(document.body)
        JSONAssert.assertEquals(body, document.body, true)
    }

    assertEquals(version, document.version)
}

fun <T> assertEntity(document: JsonEntity<T>, id: DocumentKey, entity: T?, version: Long) {
    assertEquals(id, document.id)

    if (entity == null) {
        assertNull(document.entity)
    } else {
        assertNotNull(document.entity)
        assertEquals(entity, document.entity)
    }

    assertEquals(version, document.version)
}

fun DocumentStore.assertUpdateDocuments(
    exactly: Int = 1,
    checked: List<Document> = emptyList(),
    updated: List<Document> = emptyList()
) =
    coVerify(exactly = exactly) {
        this@assertUpdateDocuments.updateDocuments(
            updatedDocuments = coWithArg {
                assertContentEquals(updated.toList(), it.toList())
            },
            checkedDocuments = coWithArg {
                assertContentEquals(checked.toList(), it.toList())
            }
        )
    }
