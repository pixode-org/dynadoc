package org.pixode.dynadoc.serialization

import io.mockk.coEvery
import io.mockk.mockk
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlinx.coroutines.flow.asFlow
import org.junit.jupiter.api.Assertions.assertEquals
import org.opentest4j.AssertionFailedError
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore

object TestSerializer : JsonSerializer {
    private val regex = Regex("\\{\"(?<key>(string|int))\":\"(?<value>[^\"]*)\"}")

    fun jsonFor(value: String) = "{\"string\":\"$value\"}"
    fun jsonFor(value: Int) = "{\"int\":\"$value\"}"

    override fun serialize(entity: Any): String {
        assertEquals(String::class.java, entity.javaClass)
        return jsonFor(entity as String)
    }

    override fun <T : Any> deserialize(json: String, type: KType): T {
        val match: MatchResult = checkNotNull(regex.matchEntire(json))

        val key: MatchGroup = checkNotNull(match.groups["key"])
        val value: MatchGroup = checkNotNull(match.groups["value"])

        @Suppress("UNCHECKED_CAST")
        return when (type) {
            typeOf<String>() -> {
                assertEquals(key.value, "string")
                value.value as T
            }

            typeOf<Int>() -> {
                assertEquals(key.value, "int")
                value.value.toInt() as T
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
