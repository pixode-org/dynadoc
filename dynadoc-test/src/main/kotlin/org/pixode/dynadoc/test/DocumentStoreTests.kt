@file:Suppress("FunctionName")

package org.pixode.dynadoc.test

import java.util.UUID
import java.util.stream.Stream
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.SortDirection
import org.pixode.dynadoc.core.SortKeyBound
import org.pixode.dynadoc.core.SortKeyRange
import org.pixode.dynadoc.core.UpdateConflictException
import org.pixode.dynadoc.core.getDocument
import org.pixode.dynadoc.core.parseDocument
import org.pixode.dynadoc.core.updateDocuments
import org.pixode.dynadoc.test.DocumentStoreTests.MethodSources.PREFIX

const val JSON_1 = """ {"abc":"def"} """
const val JSON_2 = """ {"ghi":"jkl"} """
const val JSON_3 = """ {"mno":"pqr"} """
const val JSON_4 = """ {"stu":"vwx"} """
const val JSON_5 = """ {"yza":"bcd"} """
const val JSON_6 = """ {"efg":"hij"} """
const val JSON_7 = """ {"klm":"nop"} """
const val JSON_8 = """ {"qrs":"tuv"} """

/**
 * The tests that every [DocumentStore] implementation must pass.
 */
abstract class DocumentStoreTests {
    protected abstract val store: DocumentStore
    protected val partitionKey: String = UUID.randomUUID().toString()
    protected val ids: List<DocumentKey> = (0..10).map { i -> DocumentKey("${partitionKey}_$i", "0000") }

    //region updateDocuments

    //region updateDocuments: Single document

    @ParameterizedTest
    @MethodSource("$PREFIX#updateDocuments_oneArgument")
    fun updateDocuments_emptyToValue(to: String?) = runBlocking {
        updateDocument(to, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], to, 1)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#updateDocuments_twoArguments")
    fun updateDocuments_valueToValue(from: String?, to: String?) = runBlocking {
        updateDocument(from, 0)
        updateDocument(to, 1)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], to, 2)
    }

    @Test
    fun updateDocuments_emptyToCheck() = runBlocking {
        checkDocument(0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#updateDocuments_oneArgument")
    fun updateDocuments_valueToCheck(from: String?) = runBlocking {
        updateDocument(from, 0)
        checkDocument(1)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], from, 1)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#updateDocuments_oneArgument")
    fun updateDocuments_checkToValue(to: String?) = runBlocking {
        checkDocument(0)
        updateDocument(to, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], to, 1)
    }

    @Test
    fun updateDocuments_checkToCheck() = runBlocking {
        checkDocument(0)
        checkDocument(0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @Test
    fun updateDocuments_noUpdate() = runBlocking {
        store.updateDocuments(
            updatedDocuments = emptyList(),
            checkedDocuments = emptyList(),
        )
    }

    //endregion

    //region updateDocuments: Multiple documents

    @Test
    fun updateDocuments_multipleDocumentsSuccess() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)
        updateDocument(ids[1], JSON_2, 0)
        updateDocument(ids[2], JSON_3, 0)
        updateDocument(ids[2], JSON_3, 1)

        store.updateDocuments(
            updatedDocuments = listOf(
                parseDocument(ids[0], JSON_4, 1),
                parseDocument(ids[2], JSON_5, 2),
                parseDocument(ids[3], JSON_6, 0),
            ),
            checkedDocuments = listOf(
                parseDocument(ids[1], JSON_7, 1),
                parseDocument(ids[4], JSON_8, 0),
            ),
        )

        assertDocument(store.getDocument(ids[0]), ids[0], JSON_4, 2)
        assertDocument(store.getDocument(ids[1]), ids[1], JSON_2, 1)
        assertDocument(store.getDocument(ids[2]), ids[2], JSON_5, 3)
        assertDocument(store.getDocument(ids[3]), ids[3], JSON_6, 1)
        assertDocument(store.getDocument(ids[4]), ids[4], null, 0)
    }

    @Test
    fun updateDocuments_multipleDocumentsDeleted() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        store.updateDocuments(
            parseDocument(ids[0], null, 1),
            parseDocument(ids[1], null, 0),
        )

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], null, 2)
        assertDocument(document2, ids[1], null, 1)
    }

    //endregion

    //region updateDocuments: Special cases

    @ParameterizedTest
    @ValueSource(
        strings = [
            """ "a" """,
            """ 10 """,
            """ true """,
            """ false """,
            """ null """,
            """ ["a"] """,
            """ a """,
        ],
    )
    fun updateDocuments_invalidJson(to: String) = runBlocking {
        assertThrows<IllegalArgumentException> {
            updateDocument(to, 0)
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @Test
    fun updateDocuments_multipleDocumentsInvalidJson() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        assertThrows<IllegalArgumentException> {
            store.updateDocuments(
                parseDocument(ids[0], JSON_2, 1),
                parseDocument(ids[1], """ ["a"] """, 0),
            )
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], JSON_1, 1)
        assertDocument(document2, ids[1], null, 0)
    }

    @ParameterizedTest
    @ValueSource(strings = ["updatedTwice", "checkedTwice", "updatedAndChecked"])
    fun updateDocuments_duplicateDocument(mode: String) = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        // The versions are correct, the update is rejected because a document is included more than once
        assertThrows<IllegalArgumentException> {
            when (mode) {
                "updatedTwice" -> store.updateDocuments(
                    parseDocument(ids[1], JSON_2, 0),
                    parseDocument(ids[0], JSON_3, 1),
                    parseDocument(ids[0], JSON_4, 1),
                )

                "checkedTwice" -> store.updateDocuments(
                    updatedDocuments = listOf(parseDocument(ids[1], JSON_2, 0)),
                    checkedDocuments = listOf(parseDocument(ids[0], JSON_3, 1), parseDocument(ids[0], JSON_4, 1)),
                )

                else -> store.updateDocuments(
                    updatedDocuments = listOf(parseDocument(ids[1], JSON_2, 0), parseDocument(ids[0], JSON_3, 1)),
                    checkedDocuments = listOf(parseDocument(ids[0], JSON_4, 1)),
                )
            }
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], JSON_1, 1)
        assertDocument(document2, ids[1], null, 0)
    }

    @Test
    fun updateDocuments_trailingSpace() = runBlocking {
        // Keys that only differ by a trailing space identify different documents
        val otherId = DocumentKey("${ids[0].partitionKey} ", "${ids[0].sortKey} ")
        updateDocument(ids[0], JSON_1, 0)
        updateDocument(otherId, JSON_2, 0)

        val documents: List<Document> = store.getDocuments(listOf(ids[0], otherId)).toList()

        assertDocument(documents[0], ids[0], JSON_1, 1)
        assertDocument(documents[1], otherId, JSON_2, 1)
    }

    @Test
    fun updateDocuments_specialCharacters() = runBlocking {
        val specialId = DocumentKey("$partitionKey \"'\\ é😀", "\"'\\ é😀")
        val json = """ {"a \"'\\ é":"b \"'\\ é\n"} """
        updateDocument(specialId, json, 0)
        updateDocument(specialId, json, 1)

        val document = store.getDocument(specialId)

        assertDocument(document, specialId, json, 2)
    }

    //endregion

    //region updateDocuments: Conflict

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_conflictDocumentDoesNotExist(checkOnly: Boolean) = runBlocking {
        val exception = assertThrows<UpdateConflictException> {
            if (checkOnly) {
                checkDocument(10)
            } else {
                updateDocument(JSON_1, 10)
            }
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
        assertEquals(ids[0], exception.id)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_conflictWrongVersion(checkOnly: Boolean) = runBlocking {
        updateDocument(JSON_1, 0)

        val exception = assertThrows<UpdateConflictException> {
            if (checkOnly) {
                checkDocument(10)
            } else {
                updateDocument(JSON_2, 10)
            }
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], JSON_1, 1)
        assertEquals(ids[0], exception.id)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_conflictDocumentAlreadyExists(checkOnly: Boolean) = runBlocking {
        updateDocument(JSON_1, 0)

        val exception = assertThrows<UpdateConflictException> {
            if (checkOnly) {
                checkDocument(0)
            } else {
                updateDocument(JSON_2, 0)
            }
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], JSON_1, 1)
        assertEquals(ids[0], exception.id)
    }

    @Test
    fun updateDocuments_conflictDeletedDocument() = runBlocking {
        updateDocument(JSON_1, 0)
        updateDocument(null, 1)

        val exception = assertThrows<UpdateConflictException> {
            updateDocument(JSON_2, 0)
        }

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 2)
        assertEquals(ids[0], exception.id)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "updatedDoesNotExist",
            "updatedWrongVersion",
            "checkedDoesNotExist",
            "checkedAlreadyExists",
        ],
    )
    fun updateDocuments_multipleDocumentsConflict(mode: String) = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        val exception = assertThrows<UpdateConflictException> {
            when (mode) {
                "updatedDoesNotExist" -> store.updateDocuments(
                    parseDocument(ids[0], JSON_2, 1),
                    parseDocument(ids[1], JSON_3, 10),
                )

                "updatedWrongVersion" -> store.updateDocuments(
                    parseDocument(ids[0], JSON_2, 10),
                    parseDocument(ids[1], JSON_3, 0),
                )

                "checkedDoesNotExist" -> store.updateDocuments(
                    updatedDocuments = listOf(parseDocument(ids[0], JSON_2, 1)),
                    checkedDocuments = listOf(parseDocument(ids[1], JSON_3, 10)),
                )

                else -> store.updateDocuments(
                    updatedDocuments = listOf(parseDocument(ids[1], JSON_2, 0)),
                    checkedDocuments = listOf(parseDocument(ids[0], JSON_3, 0)),
                )
            }
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], JSON_1, 1)
        assertDocument(document2, ids[1], null, 0)
        when (mode) {
            "updatedDoesNotExist", "checkedDoesNotExist" -> assertEquals(ids[1], exception.id)
            else -> assertEquals(ids[0], exception.id)
        }
    }

    //endregion

    //endregion

    //region getDocuments

    @Test
    fun getDocuments_singleDocument() = runBlocking {
        updateDocument(JSON_1, 0)

        val documents: List<Document> = store.getDocuments(listOf(ids[0])).toList()

        assertEquals(1, documents.size)
        assertDocument(documents[0], ids[0], JSON_1, 1)
    }

    @Test
    fun getDocuments_multipleDocuments() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)
        updateDocument(ids[1], JSON_2, 0)

        val documents: List<Document> = store.getDocuments(listOf(ids[0], ids[2], ids[0], ids[1])).toList()

        assertEquals(4, documents.size)
        assertDocument(documents[0], ids[0], JSON_1, 1)
        assertDocument(documents[1], ids[2], null, 0)
        assertDocument(documents[2], ids[0], JSON_1, 1)
        assertDocument(documents[3], ids[1], JSON_2, 1)
    }

    @Test
    fun getDocuments_noDocument() = runBlocking {
        val documents: List<Document> = store.getDocuments(listOf()).toList()

        assertEquals(0, documents.size)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#getDocuments_jsonDeserialization")
    fun getDocuments_jsonDeserialization(json: String) = runBlocking {
        updateDocument(ids[0], json, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], json, 1)
    }

    //endregion

    //region getRange

    @ParameterizedTest
    @MethodSource("$PREFIX#getRange_bounds")
    fun getRange_bounds(range: SortKeyRange, direction: SortDirection, expectedIndices: IntRange) = runBlocking {
        val documents = createDocumentRange()

        val result = store.getRange(partitionKey, range, direction)

        if (direction == SortDirection.ASCENDING) {
            assertDocuments(result.toList(), documents.slice(expectedIndices))
        } else {
            assertDocuments(result.toList(), documents.slice(expectedIndices).reversed())
        }
    }

    @Test
    fun getRange_wholePartition() = runBlocking {
        val documents = createDocumentRange()

        val result = store.getRange(partitionKey)

        assertDocuments(result.toList(), documents)
    }

    @ParameterizedTest
    @ValueSource(strings = ["a%", "a_", "a", "é", "", "c"])
    fun getRange_prefix(prefix: String) = runBlocking {
        val documents = listOf("a%1", "a%2", "a_1", "ab", "a", "b", "é", "éa", "ê")
            .map { parseDocument(DocumentKey(partitionKey, it), JSON_1, 0) }
        store.updateDocuments(*documents.toTypedArray())

        val result = store.getRange(partitionKey, SortKeyRange.fromPrefix(prefix)).toList()

        assertDocuments(result, documents.filter { it.id.sortKey.startsWith(prefix) }.sortedBy { it.id.sortKey })
    }

    @Test
    fun getRange_deletedDocuments() = runBlocking {
        updateDocument(DocumentKey(partitionKey, "A"), JSON_1, 0)
        updateDocument(DocumentKey(partitionKey, "B"), JSON_2, 0)
        updateDocument(DocumentKey(partitionKey, "A"), null, 1)
        // A document that is checked but doesn't exist
        store.updateDocuments(emptyList(), listOf(Document(DocumentKey(partitionKey, "C"), null, 0)))

        val result = store.getRange(partitionKey).toList()

        assertEquals(1, result.size)
        assertDocument(result[0], DocumentKey(partitionKey, "B"), JSON_2, 1)
    }

    @Test
    fun getRange_emptyPartition() = runBlocking {
        val result = store.getRange(partitionKey).toList()

        assertEquals(0, result.size)
    }

    @Test
    fun getRange_byteOrder() = runBlocking {
        // Sort keys are ordered by their UTF-8 encoding, regardless of the locale of the database
        val sortKeys: List<String> = listOf("B", "a", "é")
        val documents = sortKeys.map { parseDocument(DocumentKey(partitionKey, it), JSON_1, 0) }
        store.updateDocuments(*documents.reversed().toTypedArray())

        val result = store.getRange(partitionKey)

        assertDocuments(result.toList(), documents)
    }

    protected suspend fun createDocumentRange(): List<Document> {
        val documents = (0..9).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"a":$i} """, 0)
        }
        val otherDocuments = (0..9).map { i ->
            parseDocument(DocumentKey("${partitionKey}_other", "ABC0$i"), """ {"b":$i} """, 0)
        }
        // The documents are not created in the order of their sort keys
        store.updateDocuments(*(otherDocuments + documents.reversed()).toTypedArray())

        return documents
    }

    //endregion

    //region MethodSources

    object MethodSources {
        const val PREFIX: String = $$"org.pixode.dynadoc.test.DocumentStoreTests$MethodSources"

        @JvmStatic
        fun updateDocuments_oneArgument(): Stream<String?> {
            return Stream.of(
                JSON_1,
                null,
            )
        }

        @JvmStatic
        fun updateDocuments_twoArguments(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(JSON_1, JSON_2),
                Arguments.of(null, JSON_2),
                Arguments.of(JSON_1, null),
                Arguments.of(null, null),
            )
        }

        @JvmStatic
        fun getDocuments_jsonDeserialization(): Stream<String> {
            val scalars: List<String> = listOf(
                """ 1234567890.25 """,
                """ 42 """,
                """ "text" """,
                """ true """,
                """ false """,
                """ null """,
            )

            val firstLevel: List<String> = scalars.map {
                """ { "a": $it } """
            } + """ { } """

            val nestedObjects = firstLevel.map {
                """ { "b": $it } """
            }

            val arrayOfObjects = (scalars + firstLevel).map {
                val repeat = "$it, $it, $it"
                """ { "b": [ $repeat ] } """
            }

            val mixedArray = """ { "c": [ ${(scalars + firstLevel).joinToString()} ] } """

            return (firstLevel + nestedObjects + arrayOfObjects + mixedArray).stream()
        }

        // Every combination of start bound, end bound and direction, over the documents ABC00 to ABC09
        @JvmStatic
        fun getRange_bounds(): Stream<Arguments> {
            // The bound, and the index of the first (for a start bound) or last (for an end bound) document included
            val starts: List<Pair<SortKeyBound, Int>> = listOf(
                SortKeyBound.Unbounded to 0,
                SortKeyBound.Inclusive("ABC03") to 3,
                SortKeyBound.Exclusive("ABC03") to 4,
            )
            val ends: List<Pair<SortKeyBound, Int>> = listOf(
                SortKeyBound.Unbounded to 9,
                SortKeyBound.Inclusive("ABC06") to 6,
                SortKeyBound.Exclusive("ABC06") to 5,
            )

            return starts.flatMap { (start, first) ->
                ends.flatMap { (end, last) ->
                    SortDirection.entries.map { direction ->
                        Arguments.of(SortKeyRange(start, end), direction, first..last)
                    }
                }
            }.stream()
        }
    }

    //endregion

    //region Helper Methods

    protected suspend fun updateDocument(body: String?, version: Long) =
        updateDocument(ids[0], body, version)

    protected suspend fun updateDocument(id: DocumentKey, body: String?, version: Long) =
        store.updateDocuments(parseDocument(id, body, version))

    protected suspend fun checkDocument(version: Long) =
        store.updateDocuments(
            updatedDocuments = emptyList(),
            checkedDocuments = listOf(parseDocument(ids[0], """ {"ignored":"ignored"} """, version)),
        )

    //endregion
}
