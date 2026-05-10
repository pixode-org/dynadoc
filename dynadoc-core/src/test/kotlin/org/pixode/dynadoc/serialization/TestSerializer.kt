package org.pixode.dynadoc.serialization

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.opentest4j.AssertionFailedError
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlin.test.assertFails

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
}
