package org.pixode.dynadoc.serialization

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import kotlin.reflect.KType
import kotlin.reflect.typeOf

object TestSerializer : JsonSerializer {
    private val regex = Regex("\\{\"string\":\"(?<value>[^\"]*)\"}")

    fun jsonFor(value: String) = "{\"string\":\"$value\"}"

    override fun serialize(entity: Any): String {
        assertEquals(String::class.java, entity.javaClass)
        return jsonFor(entity as String)
    }

    override fun <T : Any> deserialize(json: String, type: KType): T {
        assertEquals(typeOf<String>(), type)

        val match: MatchResult? = regex.matchEntire(json)
        assertNotNull(match)

        val result = match!!.groups["value"]
        assertNotNull(result)

        @Suppress("UNCHECKED_CAST")
        return result!!.value as T
    }
}
