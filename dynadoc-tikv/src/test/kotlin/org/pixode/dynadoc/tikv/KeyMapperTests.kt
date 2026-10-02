package org.pixode.dynadoc.tikv

import java.security.MessageDigest
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.pixode.dynadoc.core.DocumentKey

class KeyMapperTests {
    private val keyMapper = KeyMapper("namespace")

    @Test
    fun fromDocumentKey_layout() {
        val key: ByteArray = keyMapper.fromDocumentKey(DocumentKey("partition", "local"))

        val expected: ByteArray =
            sha256("namespace").copyOf(8) +
            sha256("partition").copyOf(16) +
            "local".toByteArray()

        assertArrayEquals(expected, key)
    }

    @Test
    fun fromDocumentKey_utf8() {
        val key: ByteArray = keyMapper.fromDocumentKey(DocumentKey("é", "ü"))

        assertEquals(8 + 16 + 2, key.size)
        assertArrayEquals(byteArrayOf(0xC3.toByte(), 0xBC.toByte()), key.copyOfRange(24, 26))
    }

    @Test
    fun fromDocumentKey_samePartition() {
        val key1: ByteArray = keyMapper.fromDocumentKey(DocumentKey("partition", "a"))
        val key2: ByteArray = keyMapper.fromDocumentKey(DocumentKey("partition", "b"))

        assertArrayEquals(key1.copyOf(24), key2.copyOf(24))
        assertArrayEquals(keyMapper.partitionPrefix("partition"), key1.copyOf(24))
        assertTrue(compare(key1, key2) < 0)
    }

    @Test
    fun fromDocumentKey_differentPartitions() {
        val key1: ByteArray = keyMapper.fromDocumentKey(DocumentKey("partition1", "a"))
        val key2: ByteArray = keyMapper.fromDocumentKey(DocumentKey("partition2", "a"))

        assertFalse(key1.contentEquals(key2))
    }

    @Test
    fun fromDocumentKey_differentNamespaces() {
        val key1: ByteArray = KeyMapper("a").fromDocumentKey(DocumentKey("partition", "a"))
        val key2: ByteArray = KeyMapper("b").fromDocumentKey(DocumentKey("partition", "a"))

        assertArrayEquals(key1.copyOfRange(8, key1.size), key2.copyOfRange(8, key2.size))
        assertFalse(key1.copyOf(8).contentEquals(key2.copyOf(8)))
    }

    @Test
    fun fromDocumentKey_longNamespace() {
        val key: ByteArray = KeyMapper("a".repeat(1000)).fromDocumentKey(DocumentKey("partition", "a"))

        assertEquals(8 + 16 + 1, key.size)
    }

    @ParameterizedTest
    @CsvSource(
        value = [
            "0102   | 0103",
            "01FF   | 02",
            "01FFFF | 02",
            "FE     | FF",
            "FF     | ''",
            "FFFF   | ''",
            "''     | ''",
        ],
        delimiter = '|',
    )
    fun prefixEnd_values(prefix: String, expected: String) {
        assertArrayEquals(hex(expected), prefixEnd(hex(prefix)))
    }

    private fun sha256(value: String): ByteArray =
        MessageDigest.getInstance("SHA-256").digest(value.toByteArray())

    private fun hex(value: String): ByteArray =
        value.chunked(2).map { it.toInt(16).toByte() }.toByteArray()

    private fun compare(left: ByteArray, right: ByteArray): Int {
        for (i in 0 until minOf(left.size, right.size)) {
            val result = (left[i].toInt() and 0xFF).compareTo(right[i].toInt() and 0xFF)
            if (result != 0) {
                return result
            }
        }
        return left.size.compareTo(right.size)
    }
}
