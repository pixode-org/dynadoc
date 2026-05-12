package org.pixode.dynadoc.serialization

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertNull
import org.pixode.dynadoc.assertEntity
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore

class EntityKeyTests {
    private val documentStore: DocumentStore = TestSerializer.createMockDocumentStore()
    private val store: EntityStore = EntityStore(documentStore, TestSerializer)

    //region get

    @Test
    fun getEntity_single() = runBlocking {
        val result = store.get(StringEntityKey(1))

        assertNotNull(result)
        assertEntity(result, ids[1], "document_1", 1)
    }

    @Test
    fun getEntity_singleNull() = runBlocking {
        val result = store.get(StringEntityKey(1, isNull = true))

        assertNull(result)
    }

    @Test
    fun getEntities_pair() = runBlocking {
        val (result1, result2) = store.get(StringEntityKey(1), IntEntityKey(2))

        assertNotNull(result1)
        assertEntity(result1, ids[1], "document_1", 1)
        assertNotNull(result2)
        assertEntity(result2, idsInt[2], 2, 2)
    }

    @Test
    fun getEntities_pairNull() = runBlocking {
        val (result1, result2) = store.get(StringEntityKey(1, isNull = true), IntEntityKey(2, isNull = true))

        assertNull(result1)
        assertNull(result2)
    }

    @Test
    fun getEntities_triple() = runBlocking {
        val (result1, result2, result3) = store.get(StringEntityKey(1), IntEntityKey(2), StringEntityKey(3))

        assertNotNull(result1)
        assertEntity(result1, ids[1], "document_1", 1)
        assertNotNull(result2)
        assertEntity(result2, idsInt[2], 2, 2)
        assertNotNull(result3)
        assertEntity(result3, ids[3], "document_3", 3)
    }

    @Test
    fun getEntities_tripleNull() = runBlocking {
        val (result1, result2, result3) = store.get(
            StringEntityKey(1, isNull = true),
            IntEntityKey(2, isNull = true),
            IntEntityKey(3, isNull = true),
        )

        assertNull(result1)
        assertNull(result2)
        assertNull(result3)
    }

    @Test
    fun getEntities_list() = runBlocking {
        val result = store.get(
            StringEntityKey(1),
            StringEntityKey(2, isNull = true),
            StringEntityKey(3),
            StringEntityKey(4),
        )

        assertEquals(4, result.size)
        assertNotNull(result[0])
        assertEntity(result[0]!!, ids[1], "document_1", 1)
        assertNull(result[1])
        assertNotNull(result[2])
        assertEntity(result[2]!!, ids[3], "document_3", 3)
        assertNotNull(result[3])
        assertEntity(result[3]!!, ids[4], "document_4", 4)
    }

    //endregion

    data class StringEntityKey(val index: Int, val isNull: Boolean = false) : EntityKey<String> {
        override fun toDocumentKey() = DocumentKey("document_$index", if (isNull) "NULL" else "STRING")
    }

    data class IntEntityKey(val index: Int, val isNull: Boolean = false) : EntityKey<Int> {
        override fun toDocumentKey() = DocumentKey(index.toString(), if (isNull) "NULL" else "INT")
    }
}
