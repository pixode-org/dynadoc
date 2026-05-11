package org.pixode.dynadoc.serialization

import org.pixode.dynadoc.core.DocumentKey
import kotlin.reflect.typeOf

interface EntityKey<out E> {
    fun toDocumentKey(): DocumentKey
}


suspend inline fun <K : EntityKey<E>, reified E : Any> EntityStore.getEntity(key: K): JsonEntity<E>? =
    getEntity<E>(key.toDocumentKey()).ifExists()

@Suppress("UNCHECKED_CAST")
suspend inline fun <
    K1 : EntityKey<E1>,
    reified E1 : Any,
    K2 : EntityKey<E2>,
    reified E2 : Any,
> EntityStore.getEntities(key1: K1, key2: K2): Pair<JsonEntity<E1>?, JsonEntity<E2>?> {
    val result = getEntities(
        listOf(
            key1.toDocumentKey() to typeOf<E1>(),
            key2.toDocumentKey() to typeOf<E2>(),
        )
    )
    return Pair(
        (result[0] as JsonEntity<E1?>).ifExists(),
        (result[1] as JsonEntity<E2?>).ifExists(),
    )
}

@Suppress("UNCHECKED_CAST")
suspend inline fun <
    K1 : EntityKey<E1>,
    reified E1 : Any,
    K2 : EntityKey<E2>,
    reified E2 : Any,
    K3 : EntityKey<E3>,
    reified E3 : Any,
> EntityStore.getEntities(key1: K1, key2: K2, key3: K3): Triple<JsonEntity<E1>?, JsonEntity<E2>?, JsonEntity<E3>?> {
    val result = getEntities(
        listOf(
            key1.toDocumentKey() to typeOf<E1>(),
            key2.toDocumentKey() to typeOf<E2>(),
            key3.toDocumentKey() to typeOf<E3>()
        )
    )
    return Triple(
        (result[0] as JsonEntity<E1?>).ifExists(),
        (result[1] as JsonEntity<E2?>).ifExists(),
        (result[2] as JsonEntity<E3?>).ifExists(),
    )
}
