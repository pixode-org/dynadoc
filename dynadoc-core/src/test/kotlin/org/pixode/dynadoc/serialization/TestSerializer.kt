package org.pixode.dynadoc.serialization

import io.mockk.coEvery
import io.mockk.mockk
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlinx.coroutines.flow.asFlow
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assertions.assertEquals
import org.opentest4j.AssertionFailedError
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore

object TestSerializer : JsonSerializer {
    fun jsonFor(value: String) = JsonObject(mapOf("string" to JsonPrimitive(value)))
    fun jsonFor(value: Int) = JsonObject(mapOf("int" to JsonPrimitive(value)))

    override fun serialize(entity: Any): JsonElement {
        assertEquals(String::class.java, entity.javaClass)
        return jsonFor(entity as String)
    }

    override fun <T : Any> deserialize(json: JsonElement, type: KType): T {
        require(json is JsonObject)

        val entries = json.toMap().entries
        require(entries.size == 1)
        val (key: String, value: JsonElement) = entries.first()

        @Suppress("UNCHECKED_CAST")
        return when (type) {
            typeOf<String>() -> {
                assertEquals(key, "string")
                value.jsonPrimitive.content as T
            }

            typeOf<Int>() -> {
                assertEquals(key, "int")
                value.jsonPrimitive.content.toInt() as T
            }

            else -> throw AssertionFailedError("Invalid type: $type")
        }
    }

    fun createMockDocumentStore(): DocumentStore = mockk {
        coEvery { updateDocuments(any(), any()) } returns Unit
        coEvery { getDocuments(any()) } answers {
            firstArg<Iterable<DocumentKey>>()
                .mapIndexed { index, key ->
                    Document(
                        id = key,
                        body = when (key.sortKey) {
                            "STRING" -> jsonFor(key.partitionKey)
                            "INT" -> jsonFor(key.partitionKey.toInt())
                            else -> null
                        },
                        version = index + 1L,
                    )
                }
                .asFlow()
        }
    }
}
