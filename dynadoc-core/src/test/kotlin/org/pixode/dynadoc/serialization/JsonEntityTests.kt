package org.pixode.dynadoc.serialization

import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.pixode.dynadoc.assertEntity
import org.pixode.dynadoc.core.DocumentKey

private val id: DocumentKey = DocumentKey("PK", "SK")

class JsonEntityTests {
    @Test
    fun modify_nonNull() {
        val entity = JsonEntity(id, "abc", 1)

        val result = entity.modify { "def" }

        assertEntity(result, id, "def", 1)
    }

    @Test
    fun modify_null() {
        val entity = JsonEntity(id, "abc", 1)

        val result = entity.modify { null }

        assertEntity(result, id, null, 1)
    }

    @Test
    fun createEntity_nonNull() {
        val result: JsonEntity<String> = createEntity("PK", "SK", "abc")

        assertEntity(result, id, "abc", 0)
    }

    @Test
    fun ifExists_nonNull() {
        val entity: JsonEntity<String?> = JsonEntity<String?>(id, "abc", 1)

        val result: JsonEntity<String>? = entity.ifExists()

        assertNotNull(result)
        assertEntity(result!!, id, "abc", 1)
    }

    @Test
    fun ifExists_null() {
        val entity: JsonEntity<String?> = JsonEntity<String?>(id, null, 1)

        val result = entity.ifExists()

        assertNull(result)
    }
}
