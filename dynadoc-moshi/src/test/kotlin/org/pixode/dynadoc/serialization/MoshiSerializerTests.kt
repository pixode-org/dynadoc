package org.pixode.dynadoc.serialization

import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.skyscreamer.jsonassert.JSONAssert
import kotlin.reflect.typeOf

class MoshiSerializerTests {
    @Test
    fun serialize_string() {
        val result: String = DefaultJsonSerializer.serialize(testObject)

        JSONAssert.assertEquals(testJson, result, true)
    }

    @Test
    fun deserialize_string() {
        val result: TestClass = DefaultJsonSerializer.deserialize(testJson, typeOf<TestClass>())

        assertEquals(testObject, result)
    }

    @Test
    fun toDocument_document() {
        val document: JsonEntity<TestClass> = JsonEntity(
            id = DocumentKey("PK", "SK"),
            entity = testObject,
            version = 1,
        )

        val result: Document = DefaultJsonSerializer.toDocument(document)

        JSONAssert.assertEquals(testJson, result.body, true)
    }

    @Test
    fun fromDocument_document() {
        val document = Document(
            id = DocumentKey("PK", "SK"),
            body = testJson,
            version = 1,
        )

        val result: JsonEntity<TestClass?> = DefaultJsonSerializer.fromDocument(document)

        assertEquals(testObject, result.entity)
    }
}


private data class TestClass(
    val stringKey: String,
    val numberKey: Int,
    val booleanKey: Boolean,
    val listKey: List<Long>,
    val mapKey: Map<String, Long>,
    val nullStringKey: String?,
    val nullNumberKey: Int?,
    val nullBooleanKey: Boolean?,
    val nullListKey: List<Long>?,
    val nullMapKey: Map<String, Long>?,
)

private val testObject = TestClass(
    stringKey = "value",
    numberKey = 999,
    booleanKey = true,
    listKey = listOf(10, 20, 30),
    mapKey = mapOf(
        "a" to 1,
        "b" to 2,
    ),
    nullStringKey = null,
    nullNumberKey = null,
    nullBooleanKey = null,
    nullListKey = null,
    nullMapKey = null,
)

private val testJson = """
    |  {
    |    "stringKey": "value",
    |    "numberKey": 999,
    |    "booleanKey": true,
    |    "listKey": [10, 20, 30],
    |    "mapKey": {
    |      "a": 1,
    |      "b": 2
    |    },
    |    "nullStringKey": null,
    |    "nullNumberKey": null,
    |    "nullBooleanKey": null,
    |    "nullListKey": null,
    |    "nullMapKey": null
    |  }
""".trimMargin()
