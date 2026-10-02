package org.pixode.dynadoc.tikv

import java.security.MessageDigest
import org.pixode.dynadoc.core.DocumentKey

const val NAMESPACE_HASH_LENGTH = 8
const val PARTITION_HASH_LENGTH = 16

/**
 * Maps document keys to TiKV keys.
 *
 * A TiKV key is the concatenation of the first [NAMESPACE_HASH_LENGTH] bytes of the SHA-256 hash of the namespace,
 * the first [PARTITION_HASH_LENGTH] bytes of the SHA-256 hash of the partition key, and the local key, all strings
 * being encoded in UTF-8. Partitions are therefore spread across the key space of the namespace, while the documents
 * of a partition are stored together and sorted by local key.
 */
class KeyMapper(namespace: String) {
    private val prefix: ByteArray = hash(namespace, NAMESPACE_HASH_LENGTH)

    fun fromDocumentKey(id: DocumentKey): ByteArray =
        partitionPrefix(id.partitionKey) + id.localKey.toByteArray(Charsets.UTF_8)

    fun partitionPrefix(partitionKey: String): ByteArray =
        prefix + hash(partitionKey, PARTITION_HASH_LENGTH)

    private fun hash(value: String, length: Int): ByteArray =
        MessageDigest.getInstance("SHA-256").digest(value.toByteArray(Charsets.UTF_8)).copyOf(length)
}

/**
 * Returns the smallest key greater than all the keys starting with the given prefix, or an empty array if there is
 * no such key.
 */
fun prefixEnd(prefix: ByteArray): ByteArray {
    val lastIndex: Int = prefix.indexOfLast { it != 0xFF.toByte() }

    return if (lastIndex < 0) {
        ByteArray(0)
    } else {
        prefix.copyOf(lastIndex + 1).also { it[lastIndex]++ }
    }
}
