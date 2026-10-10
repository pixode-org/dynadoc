package org.pixode.dynadoc.core

data class SortKeyRange(
    val start: SortKeyBound,
    val end: SortKeyBound,
) {
    companion object {
        val UNBOUNDED = SortKeyRange(SortKeyBound.Unbounded, SortKeyBound.Unbounded)

        /**
         * Converts a prefix to the range of the sort keys starting with it. The start bound is the prefix itself and the end
         * bound, which is excluded, is the smallest string greater than all the strings starting with the prefix, in the
         * order of the code points, which is the order of the UTF-8 encoding used by the stores. The end bound is unbounded
         * when there is no such string, and the range is unbounded when the prefix is empty.
         */
        fun fromPrefix(prefix: String): SortKeyRange {
            // All the sort keys start with an empty prefix, which can't be used as a bound as a sort key can't be empty
            if (prefix.isEmpty()) {
                return UNBOUNDED
            }

            val codePoints: IntArray = prefix.codePoints().toArray()

            // The greatest code point can't be incremented, so it is dropped with the ones that follow it
            val length: Int = codePoints.indexOfLast { it != Character.MAX_CODE_POINT } + 1

            // There is no string greater than all the strings starting with a prefix made of the greatest code points
            if (length == 0) {
                return SortKeyRange(SortKeyBound.Inclusive(prefix), SortKeyBound.Unbounded)
            }

            // The code points of the surrogates, which are not valid, are skipped
            val last: Int = codePoints[length - 1]
            codePoints[length - 1] = if (last == Char.MIN_SURROGATE.code - 1) Char.MAX_SURROGATE.code + 1 else last + 1

            return SortKeyRange(SortKeyBound.Inclusive(prefix), SortKeyBound.Exclusive(String(codePoints, 0, length)))
        }
    }
}

sealed interface SortKeyBound {
    data class Inclusive(val value: String) : SortKeyBound
    data class Exclusive(val value: String) : SortKeyBound
    data object Unbounded : SortKeyBound
}

enum class SortDirection {
    ASCENDING,
    DESCENDING,
}

