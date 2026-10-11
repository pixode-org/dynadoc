package org.pixode.dynadoc.tidb

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class TiDbTableTests {
    // The expected values are those returned by the CRC32 function of TiDB, which hashes the UTF-8 encoding
    @ParameterizedTest
    @CsvSource(
        value = [
            "'',0",
            "a,3904355907",
            "123456789,3421780262",
            "products,3015334490",
            "'a b ',4130742391",
            "é,235179326",
            "日本語,2819314405",
            "😀,88978756",
        ],
    )
    fun partitionHash_knownValues(partitionKey: String, expected: Long) {
        assertEquals(expected, partitionHash(partitionKey))
    }

    @Test
    fun selectDocumentsSql_statementCached() {
        val table = TiDbTable("documents")

        val first = table.selectDocumentsSql(3)
        val second = table.selectDocumentsSql(3)

        val expected = """
            SELECT partition_key, sort_key, version, CAST(body AS CHAR) AS body FROM `documents`
            WHERE (partition_hash, partition_key, sort_key) IN ((?, ?, ?), (?, ?, ?), (?, ?, ?))
        """.trimIndent().replace('\n', ' ')
        assertEquals(expected, first)
        assertSame(first, second)
    }

    @Test
    fun selectDocumentsSql_statementNotCached() {
        val table = TiDbTable("documents")

        val first = table.selectDocumentsSql(20)
        val second = table.selectDocumentsSql(20)

        val expected = """
            SELECT partition_key, sort_key, version, CAST(body AS CHAR) AS body FROM `documents`
            WHERE (partition_hash, partition_key, sort_key) IN (${List(20) { "(?, ?, ?)" }.joinToString()})
        """.trimIndent().replace('\n', ' ')
        assertEquals(expected, first)
        assertEquals(expected, second)
        assertNotSame(first, second)
    }
}
