package org.pixode.dynadoc.tidb

import org.junit.jupiter.api.Assertions.assertEquals
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
}
