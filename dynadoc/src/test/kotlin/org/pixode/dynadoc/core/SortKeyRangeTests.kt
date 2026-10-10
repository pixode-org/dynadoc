package org.pixode.dynadoc.core

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SortKeyRangeTests {
    @Test
    fun fromPrefix_emptyPrefix() {
        assertEquals(SortKeyRange.UNBOUNDED, SortKeyRange.fromPrefix(""))
    }

    @Test
    fun fromPrefix_incrementsLastCodePoint() {
        assertEquals(
            SortKeyRange(SortKeyBound.Inclusive("ab-"), SortKeyBound.Exclusive("ab.")),
            SortKeyRange.fromPrefix("ab-"),
        )
        // The code points are incremented, rather than the UTF-16 characters: U+1F600 is followed by U+1F601
        assertEquals(
            SortKeyRange(SortKeyBound.Inclusive("a\uD83D\uDE00"), SortKeyBound.Exclusive("a\uD83D\uDE01")),
            SortKeyRange.fromPrefix("a\uD83D\uDE00"),
        )
    }

    @Test
    fun fromPrefix_skipsSurrogates() {
        // U+D7FF is followed by U+E000
        assertEquals(
            SortKeyRange(SortKeyBound.Inclusive("a\uD7FF"), SortKeyBound.Exclusive("a\uE000")),
            SortKeyRange.fromPrefix("a\uD7FF"),
        )
    }

    @Test
    fun fromPrefix_greatestCodePoints() {
        // U+10FFFF
        val max = "\uDBFF\uDFFF"

        // The greatest code points are dropped, and the one before them is incremented
        assertEquals(
            SortKeyRange(SortKeyBound.Inclusive("ab$max$max"), SortKeyBound.Exclusive("ac")),
            SortKeyRange.fromPrefix("ab$max$max"),
        )
        assertEquals(
            SortKeyRange(SortKeyBound.Inclusive(max), SortKeyBound.Unbounded),
            SortKeyRange.fromPrefix(max),
        )
    }

    @Test
    fun fromPrefix_lastBasicMultilingualPlaneCodePoint() {
        // The next code point is the first one outside of the basic multilingual plane: U+FFFF is followed by
        // U+10000
        assertEquals(
            SortKeyRange(SortKeyBound.Inclusive("a\uFFFF"), SortKeyBound.Exclusive("a\uD800\uDC00")),
            SortKeyRange.fromPrefix("a\uFFFF"),
        )
    }

    @Test
    fun fromPrefix_carriesToHighSurrogate() {
        // U+1FFFF is followed by U+20000
        assertEquals(
            SortKeyRange(SortKeyBound.Inclusive("a\uD83F\uDFFF"), SortKeyBound.Exclusive("a\uD840\uDC00")),
            SortKeyRange.fromPrefix("a\uD83F\uDFFF"),
        )
    }
}
