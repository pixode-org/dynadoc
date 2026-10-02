package org.pixode.dynadoc.tikv

import java.net.URI
import java.util.UUID
import java.util.stream.Stream
import kotlin.concurrent.thread
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.UpdateConflictException
import org.pixode.dynadoc.core.getDocument
import org.pixode.dynadoc.core.parseDocument
import org.pixode.dynadoc.core.updateDocuments
import org.pixode.dynadoc.tikv.TiKVDocumentStoreTests.MethodSources.PREFIX
import org.testcontainers.containers.FixedHostPortGenericContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.tikv.common.TiConfiguration
import org.tikv.common.TiSession
import org.tikv.common.meta.TiTimestamp
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.txn.TwoPhaseCommitter

private const val JSON_1 = """ {"abc":"def"} """
private const val JSON_2 = """ {"ghi":"jkl"} """
private const val JSON_3 = """ {"mno":"pqr"} """

class TiKVDocumentStoreTests {
    private val store: TiKVDocumentStore = TiKVDocumentStore(session, NAMESPACE)
    private val partitionKey: String = UUID.randomUUID().toString()
    private val ids: List<DocumentKey> = (0..10).map { i -> DocumentKey("${partitionKey}_$i", "0000") }
    private val keyMapper = KeyMapper(NAMESPACE)
    private val valueMapper = ValueMapper()

    //region updateDocuments

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
    fun updateDocuments_noReservedFields() = runBlocking {
        val json = """ {"partition_key":"a","local_key":"b","version":3,"body":null} """
        updateDocument(json, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], json, 1)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_conflictDocumentDoesNotExist(checkOnly: Boolean) = runBlocking {
        val exception = assertThrows<UpdateConflictException> {
            if (checkOnly) {
                checkDocument(10)
            } else {
                updateDocument(JSON_2, 10)
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

    @Test
    fun updateDocuments_multipleDocumentsSuccess() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)
        updateDocument(ids[1], JSON_2, 0)

        store.updateDocuments(
            updatedDocuments = listOf(
                parseDocument(ids[0], """ {"v":"1"} """, 1),
                parseDocument(ids[2], """ {"v":"2"} """, 0),
            ),
            checkedDocuments = listOf(
                parseDocument(ids[1], """ {"v":"3"} """, 1),
                parseDocument(ids[3], """ {"v":"4"} """, 0),
            ),
        )

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])
        val document3 = store.getDocument(ids[2])
        val document4 = store.getDocument(ids[3])

        assertDocument(document1, ids[0], """ {"v":"1"} """, 2)
        assertDocument(document2, ids[1], JSON_2, 1)
        assertDocument(document3, ids[2], """ {"v":"2"} """, 1)
        assertDocument(document4, ids[3], null, 0)
    }

    @Test
    fun updateDocuments_multipleDocumentsUpdatedAndChecked() = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        store.updateDocuments(
            updatedDocuments = listOf(parseDocument(ids[0], JSON_2, 1)),
            checkedDocuments = listOf(parseDocument(ids[0], JSON_1, 1)),
        )

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], JSON_2, 2)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_multipleDocumentsConflict(checkOnly: Boolean) = runBlocking {
        updateDocument(ids[0], JSON_1, 0)

        val exception = assertThrows<UpdateConflictException> {
            if (checkOnly) {
                store.updateDocuments(
                    updatedDocuments = listOf(parseDocument(ids[0], JSON_2, 1)),
                    checkedDocuments = listOf(parseDocument(ids[1], JSON_3, 10)),
                )
            } else {
                store.updateDocuments(
                    parseDocument(ids[0], JSON_2, 1),
                    parseDocument(ids[1], JSON_3, 10),
                )
            }
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], JSON_1, 1)
        assertDocument(document2, ids[1], null, 0)
        assertEquals(ids[1], exception.id)
    }

    @Test
    fun updateDocuments_multipleDocumentsCheckExistingConflict() = runBlocking {
        updateDocument(ids[1], JSON_2, 0)

        val exception = assertThrows<UpdateConflictException> {
            store.updateDocuments(
                updatedDocuments = listOf(parseDocument(ids[0], JSON_1, 0)),
                checkedDocuments = listOf(parseDocument(ids[1], JSON_3, 0)),
            )
        }

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], null, 0)
        assertDocument(document2, ids[1], JSON_2, 1)
        assertEquals(ids[1], exception.id)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_multipleDocumentsConcurrentTransaction(checkOnly: Boolean) = runBlocking {
        updateDocument(ids[0], JSON_1, 0)
        updateDocument(ids[1], JSON_2, 0)

        // Start a concurrent transaction writing the second document, and commit it after the start of the update.
        // The update reads the second document at its initial version, but its write conflicts with the commit.
        val concurrentKey: ByteArray = keyMapper.fromDocumentKey(ids[1])
        val concurrentValue: ByteArray = valueMapper.fromDocument(parseDocument(ids[1], JSON_3, 1))
        val committer = TwoPhaseCommitter(session, session.timestamp.version)
        committer.prewritePrimaryKey(ConcreteBackOffer.newCustomBackOff(5000), concurrentKey, concurrentValue)

        val commitThread = thread {
            Thread.sleep(1000)
            val commitTs: TiTimestamp = session.timestamp
            committer.commitPrimaryKey(ConcreteBackOffer.newCustomBackOff(5000), concurrentKey, commitTs.version)
        }

        val exception = assertThrows<UpdateConflictException> {
            if (checkOnly) {
                store.updateDocuments(
                    updatedDocuments = listOf(parseDocument(ids[0], JSON_3, 1)),
                    checkedDocuments = listOf(parseDocument(ids[1], JSON_1, 1)),
                )
            } else {
                store.updateDocuments(
                    parseDocument(ids[0], JSON_3, 1),
                    parseDocument(ids[1], JSON_1, 1),
                )
            }
        }

        commitThread.join()
        committer.close()

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], JSON_1, 1)
        assertDocument(document2, ids[1], JSON_3, 2)
        assertEquals(ids[1], exception.id)
    }

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

    @Test
    fun getDocuments_otherKeyPrefix() = runBlocking {
        updateDocument(JSON_1, 0)

        val document = TiKVDocumentStore(session, "other").getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @ParameterizedTest
    @MethodSource("$PREFIX#getDocuments_jsonDeserialization")
    fun getDocuments_jsonDeserialization(json: String) = runBlocking {
        updateDocument(ids[0], json, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], json, 1)
    }

    //endregion

    //region scan

    @Test
    fun scan_range() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"a":$i} """, 0)
        }
        store.updateDocuments(*documents.toTypedArray())

        val result = store.scan(partitionKey, "ABC03", "ABC06")

        assertDocuments(result.toList(), documents.slice(3..5))
    }

    @Test
    fun scan_wholePartition() = runBlocking {
        val documents = (0..9).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC0$i"), """ {"a":$i} """, 0)
        }
        // Documents with the same local keys in another partition
        val otherDocuments = (0..9).map { i ->
            parseDocument(DocumentKey("${partitionKey}_other", "ABC0$i"), """ {"b":$i} """, 0)
        }
        store.updateDocuments(*(otherDocuments + documents.reversed()).toTypedArray())

        val result = store.scan(partitionKey)

        assertDocuments(result.toList(), documents)
    }

    @Test
    fun scan_deletedDocuments() = runBlocking {
        updateDocument(DocumentKey(partitionKey, "A"), JSON_1, 0)
        updateDocument(DocumentKey(partitionKey, "B"), JSON_2, 0)
        updateDocument(DocumentKey(partitionKey, "A"), null, 1)

        val result = store.scan(partitionKey).toList()

        assertEquals(2, result.size)
        assertDocument(result[0], DocumentKey(partitionKey, "A"), null, 2)
        assertDocument(result[1], DocumentKey(partitionKey, "B"), JSON_2, 1)
    }

    @Test
    fun scan_emptyPartition() = runBlocking {
        val result = store.scan(partitionKey).toList()

        assertEquals(0, result.size)
    }

    @ParameterizedTest
    @ValueSource(ints = [255, 256, 257, 600])
    fun scan_multipleBatches(count: Int) = runBlocking {
        val documents = (0 until count).map { i ->
            parseDocument(DocumentKey(partitionKey, "ABC%04d".format(i)), """ {"b":$i} """, 0)
        }
        documents.chunked(100).forEach { chunk -> store.updateDocuments(*chunk.toTypedArray()) }

        val all = store.scan(partitionKey).toList()
        val range = store.scan(partitionKey, "ABC0020", "ABC%04d".format(count - 10)).toList()

        assertDocuments(all, documents)
        assertDocuments(range, documents.slice(20 until count - 10))
    }

    //endregion

    //region MethodSources

    object MethodSources {
        const val PREFIX: String = $$"org.pixode.dynadoc.tikv.TiKVDocumentStoreTests$MethodSources"

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
                """ 1234567890.0987654321 """,
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
    }

    //endregion

    //region Helper Methods

    private suspend fun updateDocument(body: String?, version: Long) =
        updateDocument(ids[0], body, version)

    private suspend fun updateDocument(id: DocumentKey, body: String?, version: Long) =
        store.updateDocuments(parseDocument(id, body, version))

    private suspend fun checkDocument(version: Long) =
        store.updateDocuments(
            updatedDocuments = emptyList(),
            checkedDocuments = listOf(parseDocument(ids[0], """ {"ignored":"ignored"} """, version)),
        )

    private fun assertDocuments(actual: List<Document>, expected: List<Document>) {
        assertEquals(expected.size, actual.size)

        repeat(actual.size) { i ->
            assertDocument(actual[i], expected[i].id, expected[i].body.toString(), 1)
        }
    }

    //endregion

    //region Setup

    private companion object Setup {
        const val NAMESPACE = "tests"
        const val PD_PORT = 2379
        const val TIKV_PORT = 20160
        const val VERSION = "v8.5.3"

        lateinit var session: TiSession

        // The client connects to the addresses advertised by PD and TiKV, so both containers share the same network
        // namespace and advertise addresses that are also valid from the host, which requires fixed ports
        @Suppress("DEPRECATION")
        private val pd = FixedHostPortGenericContainer("pingcap/pd:$VERSION")
            .withFixedExposedPort(PD_PORT, PD_PORT)
            .withFixedExposedPort(TIKV_PORT, TIKV_PORT)
            .withCommand(
                "--name=pd",
                "--client-urls=http://0.0.0.0:$PD_PORT",
                "--advertise-client-urls=http://127.0.0.1:$PD_PORT",
                "--peer-urls=http://0.0.0.0:2380",
                "--advertise-peer-urls=http://127.0.0.1:2380",
            )
            .waitingFor(Wait.forHttp("/pd/api/v1/health").forPort(PD_PORT))

        private val tikv = GenericContainer("pingcap/tikv:$VERSION")
            .withCommand(
                "--addr=0.0.0.0:$TIKV_PORT",
                "--advertise-addr=127.0.0.1:$TIKV_PORT",
                "--status-addr=127.0.0.1:20180",
                "--pd=127.0.0.1:$PD_PORT",
                "--data-dir=/data",
            )

        @BeforeAll
        @JvmStatic
        fun globalSetup() {
            pd.start()
            tikv.withNetworkMode("container:${pd.containerId}").start()
            waitForStore()

            session = TiSession.create(TiConfiguration.createDefault("127.0.0.1:$PD_PORT"))
        }

        @AfterAll
        @JvmStatic
        fun globalTeardown() {
            session.close()
            tikv.stop()
            pd.stop()
        }

        private fun waitForStore() {
            val deadline: Long = System.currentTimeMillis() + 120_000

            while (true) {
                val stores: String = runCatching {
                    URI("http://127.0.0.1:$PD_PORT/pd/api/v1/stores").toURL().readText()
                }.getOrDefault("")

                if (Regex(""""state_name":\s*"Up"""").containsMatchIn(stores)) {
                    return
                }

                check(System.currentTimeMillis() < deadline) { tikv.logs }
                Thread.sleep(500)
            }
        }
    }

    //endregion
}
