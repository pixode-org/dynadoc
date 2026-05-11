package org.pixode.dynadoc.serialization

import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import org.pixode.dynadoc.assertEntity
import org.pixode.dynadoc.assertUpdateDocuments
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.serialization.TestSerializer.jsonFor
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.reflect.typeOf

val ids = (0..9).map { i -> DocumentKey("document_$i", "STRING") }
val idsInt = (0..9).map { i -> DocumentKey(i.toString(), "INT") }
val idsNull = (0..9).map { i -> DocumentKey("document_$i", "NULL") }

class EntityStoreTests {
    private val documentStore: DocumentStore = TestSerializer.createMockDocumentStore()
    private val store: EntityStore = EntityStore(documentStore, TestSerializer)

    //region updateEntities

    @Test
    fun updateEntities_updateToValue() = runBlocking {
        val document = JsonEntity(ids[0], "abc", 1)

        store.updateEntities(document)

        documentStore.assertUpdateDocuments(
            updated = listOf(Document(ids[0], jsonFor("abc"), 1)),
        )
    }

    @Test
    fun updateEntities_updateToNull() = runBlocking {
        val document = JsonEntity(ids[0], null, 1)

        store.updateEntities(document)

        documentStore.assertUpdateDocuments(
            updated = listOf(Document(ids[0], null, 1)),
        )
    }

    @Test
    fun updateEntities_check() = runBlocking {
        val document = JsonEntity(ids[0], 5.5f, 1)

        store.updateEntities(checkedDocuments = listOf(document))

        documentStore.assertUpdateDocuments(
            checked = listOf(Document(ids[0], null, 1)),
        )
    }

    @Test
    fun updateEntities_updateAndCheck() = runBlocking {
        store.updateEntities(
            updatedDocuments = listOf(
                JsonEntity(ids[0], "abc", 1),
                JsonEntity(ids[1], null, 2),
            ),
            checkedDocuments = listOf(
                JsonEntity(ids[2], 5.5f, 3),
                JsonEntity(ids[3], BigDecimal("21"), 4)
            )
        )

        documentStore.assertUpdateDocuments(
            updated = listOf(
                Document(ids[0], jsonFor("abc"), 1),
                Document(ids[1], null, 2),
            ),
            checked = listOf(
                Document(ids[2], null, 3),
                Document(ids[3], null, 4),
            ),
        )
    }

    //endregion

    //region getEntities

    @Test
    fun getEntities_multipleHomogeneous() = runBlocking {
        val result: List<JsonEntity<String?>> = store.getEntities(listOf(ids[0], idsNull[1], ids[2]))

        assertEquals(3, result.size)
        assertEntity(result[0], ids[0], "document_0", 1)
        assertEntity(result[1], idsNull[1], null, 2)
        assertEntity(result[2], ids[2], "document_2", 3)
    }

    @Test
    fun getEntities_multipleHeterogeneous() = runBlocking {
        val result: List<JsonEntity<Any?>> = store.getEntities(
            listOf(
                ids[0] to typeOf<String>(),
                idsNull[1] to typeOf<String>(),
                idsInt[2] to typeOf<Int>(),
                idsNull[3] to typeOf<Int>(),
            )
        )

        println(result)

        assertEquals(4, result.size)
        assertEntity(result[0], ids[0], "document_0", 1)
        assertEntity(result[1], idsNull[1], null, 2)
        assertEntity(result[2], idsInt[2], 2, 3)
        assertEntity(result[3], idsNull[3], null, 4)
    }

    @Test
    fun getEntity_notNull() = runBlocking {
        val result: JsonEntity<String?> = store.getEntity(ids[0])

        assertEntity(result, ids[0], "document_0", 1)
    }

    @Test
    fun getEntity_null() = runBlocking {
        val result: JsonEntity<String?> = store.getEntity(idsNull[0])

        assertEntity(result, idsNull[0], null, 1)
    }

    //endregion
}
