package org.pixode.dynadoc.serialization

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.pixode.dynadoc.assertUpdateDocuments
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.UpdateConflictException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class TransactionTests {
    private val documentStore: DocumentStore = mockk {
        coEvery { updateDocuments(any(), any()) } returns Unit
    }
    private val store: EntityStore = EntityStore(documentStore, TestSerializer)

    //region transaction

    @Test
    fun transaction_commit() = runBlocking {
        val result: String = store.transaction {
            check(JsonEntity(ids[0], "abc", 1))
            modify(JsonEntity(ids[1], "abc", 2))
            "result"
        }

        Assertions.assertEquals("result", result)
        documentStore.assertUpdateDocuments(
            checked = listOf(Document(ids[0], null, 1)),
            updated = listOf(Document(ids[1], TestSerializer.jsonFor("abc"), 2)),
        )
    }

    @Test
    fun transaction_nonLocalReturn() = runBlocking {
        run {
            store.transaction {
                check(JsonEntity(ids[0], "abc", 1))
                modify(JsonEntity(ids[1], "abc", 2))
                return@run
            }
        }

        coVerify(exactly = 0) {
            documentStore.updateDocuments(any(), any())
        }
    }

    @Test
    fun transaction_exception() = runBlocking {
        assertThrows<ArithmeticException>(
            fun() = runBlocking {
                store.transaction {
                    check(JsonEntity(ids[0], "abc", 1))
                    modify(JsonEntity(ids[1], "abc", 2))
                    throw ArithmeticException()
                }
            })

        coVerify(exactly = 0) {
            documentStore.updateDocuments(any(), any())
        }
    }

    @Test
    fun transaction_conflictNoRetry() = runBlocking {
        coEvery {
            documentStore.updateDocuments(any(), any())
        } throws UpdateConflictException(ids[0])

        assertThrows<UpdateConflictException>(
            fun() = runBlocking {
                store.transaction(NO_RETRY) {
                    check(JsonEntity(ids[0], "abc", 1))
                    modify(JsonEntity(ids[1], "abc", 2))
                }
            })

        documentStore.assertUpdateDocuments(
            updated = listOf(Document(ids[1], TestSerializer.jsonFor("abc"), 2)),
            checked = listOf(Document(ids[0], null, 1)),
        )
    }

    @ParameterizedTest
    @ValueSource(ints = [0, 1, 3])
    fun transaction_retryConflicts(retries: Int) = runBlocking {
        coEvery {
            documentStore.updateDocuments(any(), any())
        } throws UpdateConflictException(ids[0])

        assertThrows<UpdateConflictException>(
            fun() = runBlocking {
                store.transaction(retry(maxRetries = retries)) {
                    check(JsonEntity(ids[0], "abc", 1))
                    modify(JsonEntity(ids[1], "abc", 2))
                }
            })

        documentStore.assertUpdateDocuments(
            exactly = retries + 1,
            updated = listOf(Document(ids[1], TestSerializer.jsonFor("abc"), 2)),
            checked = listOf(Document(ids[0], null, 1)),
        )
    }

    @Test
    fun transaction_submitException() = runBlocking {
        coEvery {
            documentStore.updateDocuments(any(), any())
        } throws IllegalArgumentException()

        assertThrows<IllegalArgumentException>(
            fun() = runBlocking {
                store.transaction(retry(maxRetries = 3)) {
                    check(JsonEntity(ids[0], "abc", 1))
                    modify(JsonEntity(ids[1], "abc", 2))
                }
            })

        documentStore.assertUpdateDocuments(
            updated = listOf(Document(ids[1], TestSerializer.jsonFor("abc"), 2)),
            checked = listOf(Document(ids[0], null, 1)),
        )
    }

    //endregion
}
