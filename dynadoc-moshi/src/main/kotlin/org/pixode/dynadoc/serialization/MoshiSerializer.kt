package org.pixode.dynadoc.serialization

import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import kotlin.reflect.KType
import kotlin.reflect.jvm.jvmErasure

class MoshiSerializer(
    private val moshi: Moshi,
) : JsonSerializer {

    @Suppress("UNCHECKED_CAST")
    override fun serialize(entity: Any): String =
        moshi.adapter(entity::class.java as Class<Any>).serializeNulls().toJson(entity)

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any> deserialize(json: String, type: KType): T =
        moshi.adapter(type.jvmErasure.java as Class<T>).fromJson(json) as T
}


val DefaultJsonSerializer = MoshiSerializer(
    Moshi.Builder()
        .addLast(KotlinJsonAdapterFactory())
        .build(),
)
