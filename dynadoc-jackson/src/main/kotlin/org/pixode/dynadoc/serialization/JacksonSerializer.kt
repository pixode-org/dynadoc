package org.pixode.dynadoc.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.reflect.KType
import kotlin.reflect.jvm.jvmErasure

class JacksonSerializer(
    private val objectMapper: ObjectMapper
) : JsonSerializer {

    override fun serialize(entity: Any): String =
        objectMapper.writeValueAsString(entity)

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any> deserialize(json: String, type: KType): T =
        objectMapper.readValue(json, type.jvmErasure.java) as T
}


val DefaultJsonSerializer = JacksonSerializer(objectMapper =
    jacksonObjectMapper {
        this.configure(KotlinFeature.StrictNullChecks, true)
        this.configure(KotlinFeature.NullIsSameAsDefault, true)
    })
