package org.pixode.dynadoc.tikv

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.DocumentStore
import org.pixode.dynadoc.core.UpdateConflictException
import org.tikv.common.BytePairWrapper
import org.tikv.common.TiSession
import org.tikv.common.exception.KeyException
import org.tikv.common.exception.RegionException
import org.tikv.common.exception.TiClientInternalException
import org.tikv.common.meta.TiTimestamp
import org.tikv.common.operation.KVErrorHandler
import org.tikv.common.operation.iterator.ConcreteScanIterator
import org.tikv.common.region.RegionManager
import org.tikv.common.region.RegionStoreClient
import org.tikv.common.region.TiRegion
import org.tikv.common.region.TiStore
import org.tikv.common.util.BackOffFunction
import org.tikv.common.util.BackOffer
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.kvproto.Kvrpcpb
import org.tikv.kvproto.Kvrpcpb.Mutation
import org.tikv.kvproto.TikvGrpc
import org.tikv.shade.com.google.protobuf.ByteString
import org.tikv.common.exception.WriteConflictException as CommonWriteConflictException
import org.tikv.txn.exception.WriteConflictException as TxnWriteConflictException

private const val LOCK_TTL_MS = 3000L
private const val WRITE_MAX_BACKOFF_MS = 20000
private const val ROLLBACK_MAX_BACKOFF_MS = 5000

/**
 * Represents an implementation of the [DocumentStore] interface that relies on the transactional API of TiKV for
 * persistence.
 *
 * Documents are stored under a key made of a hash of the namespace, a hash of the partition key and the local key
 * (see [KeyMapper]), with a JSON value holding the document key, the version and the body (see [ValueMapper]).
 * Deleted documents are kept with a null body so that their version is preserved.
 *
 * Updates rely on TiKV optimistic transactions: the documents are read at the start timestamp of the transaction to
 * check their versions, then written using the two-phase commit protocol, which fails if any of them has been
 * written by another transaction since the start timestamp. An update also fails without waiting if any of the
 * documents is locked by a concurrent update.
 */
class TiKVDocumentStore(
    private val session: TiSession,
    namespace: String,
) : DocumentStore {

    private val keyMapper: KeyMapper = KeyMapper(namespace)
    private val valueMapper: ValueMapper = ValueMapper()
    private val regionManager: RegionManager = session.regionManager

    //region updateDocuments

    override suspend fun updateDocuments(updatedDocuments: Iterable<Document>, checkedDocuments: Iterable<Document>) {
        val updatedList: List<Document> = updatedDocuments.toList()
        val checkedList: List<Document> = checkedDocuments.toList()

        if (updatedList.isNotEmpty() || checkedList.isNotEmpty()) {
            withContext(Dispatchers.IO) {
                commitTransaction(updatedList, checkedList)
            }
        }
    }

    private fun commitTransaction(updatedDocuments: List<Document>, checkedDocuments: List<Document>) {
        // Serialize first, so that an invalid document fails before anything is written
        val documents: List<RawDocument> =
            updatedDocuments.map {
                RawDocument(it, rawKey(it), ByteString.copyFrom(valueMapper.fromDocument(it)))
            } + checkedDocuments.map {
                RawDocument(it, rawKey(it), null)
            }

        val startTimestamp: TiTimestamp = session.timestamp
        val currentValues: Map<ByteString, ByteString> = batchGet(documents.map { it.rawKey }, startTimestamp)

        documents.forEach {
            if (currentVersion(currentValues[it.rawKey]) != it.document.version) {
                throw UpdateConflictException(it.document.id)
            }
        }

        val mutations = mutableMapOf<ByteString, Mutation>()
        for (document in documents) {
            if (document.rawValue != null) {
                mutations[document.rawKey] = mutation(Kvrpcpb.Op.Put, document.rawKey, document.rawValue)
            } else {
                // Lock the checked documents, so that a concurrent write to any of them causes a conflict
                mutations.getOrPut(document.rawKey) { mutation(Kvrpcpb.Op.Lock, document.rawKey, ByteString.EMPTY) }
            }
        }

        val primary: RawDocument = documents.first()
        val primaryKey: ByteString = primary.rawKey
        val backOffer: BackOffer = ConcreteBackOffer.newCustomBackOff(WRITE_MAX_BACKOFF_MS)

        try {
            // The primary key is written first, as the status of the transaction is determined by its lock
            val prewriteBackOffer = NoLockWaitBackOffer(backOffer)
            prewrite(prewriteBackOffer, primaryKey, listOf(mutations.getValue(primaryKey)), startTimestamp)
            prewrite(prewriteBackOffer, primaryKey, mutations.values.drop(1), startTimestamp)
        } catch (exception: RuntimeException) {
            // The primary key is not committed, so the transaction can be abandoned
            rollback(mutations.keys.toList(), startTimestamp)

            if (exception is LockedKeyException) {
                val locked: RawDocument = documents.firstOrNull { it.rawKey == exception.key } ?: primary
                throw UpdateConflictException(locked.document.id)
            } else if (isWriteConflict(exception)) {
                throw UpdateConflictException(findConflict(documents, currentValues))
            } else {
                throw exception
            }
        }

        val commitTimestamp: TiTimestamp = session.timestamp

        try {
            // The transaction is committed once the primary key is committed
            commit(backOffer, listOf(primaryKey), startTimestamp, commitTimestamp)
        } catch (_: KeyException) {
            // The lock has expired and the transaction has been rolled back by another transaction, or the commit
            // timestamp is too old: either way, the transaction has not been committed
            rollback(mutations.keys.toList(), startTimestamp)
            throw UpdateConflictException(primary.document.id)
        }

        try {
            commit(backOffer, mutations.keys.drop(1), startTimestamp, commitTimestamp)
        } catch (_: Exception) {
            // The remaining locks are resolved by readers using the status of the primary key
        }
    }

    private fun prewrite(
        backOffer: BackOffer,
        primaryKey: ByteString,
        mutations: List<Mutation>,
        startTimestamp: TiTimestamp,
    ) = forEachRegion(backOffer, mutations, Mutation::getKey) { region, store, regionMutations ->
        session.regionStoreClientBuilder.build(region, store)
            .prewrite(backOffer, primaryKey, regionMutations, startTimestamp.version, LOCK_TTL_MS)
    }

    private fun commit(
        backOffer: BackOffer,
        keys: List<ByteString>,
        startTimestamp: TiTimestamp,
        commitTimestamp: TiTimestamp,
    ) = forEachRegion(backOffer, keys, { it }) { region, store, regionKeys ->
        session.regionStoreClientBuilder.build(region, store)
            .commit(backOffer, regionKeys, startTimestamp.version, commitTimestamp.version)
    }

    /**
     * A [BackOffer] that fails with a [LockedKeyException] instead of waiting for a lock during a prewrite.
     *
     * The client only waits for a lock after resolving the locks of committed, rolled back or expired transactions, so
     * the remaining lock belongs to a concurrent transaction that is still running. That transaction is most likely to
     * commit, in which case this transaction would fail with a write conflict anyway, after waiting for it with an
     * exponential backoff.
     */
    private class NoLockWaitBackOffer(private val backOffer: BackOffer) : BackOffer by backOffer {
        override fun doBackOffWithMaxSleep(
            funcType: BackOffFunction.BackOffFuncType,
            maxSleepMs: Long,
            err: Exception,
        ) {
            if (funcType == BackOffFunction.BackOffFuncType.BoTxnLock) {
                val keyError: Kvrpcpb.KeyError? = (err as? KeyException)?.keyErr
                throw LockedKeyException(if (keyError?.hasLocked() == true) keyError.locked.key else null, err)
            }

            backOffer.doBackOffWithMaxSleep(funcType, maxSleepMs, err)
        }
    }

    /**
     * Indicates that a prewrite has found a key locked by a concurrent transaction.
     */
    private class LockedKeyException(val key: ByteString?, cause: Exception) :
        RuntimeException("The key is locked by a concurrent transaction", cause)

    /**
     * Rolls back the locks of an abandoned transaction, so that other transactions don't wait for them to expire.
     * This is a best effort: locks that can't be rolled back are resolved by other transactions once they expire.
     */
    private fun rollback(keys: List<ByteString>, startTimestamp: TiTimestamp) {
        val backOffer: BackOffer = ConcreteBackOffer.newCustomBackOff(ROLLBACK_MAX_BACKOFF_MS)

        try {
            forEachRegion(backOffer, keys, { it }) { region, store, regionKeys ->
                val client: RegionStoreClient = session.regionStoreClientBuilder.build(region, store)
                val errorHandler = KVErrorHandler<Kvrpcpb.BatchRollbackResponse>(
                    regionManager,
                    client,
                    client.lockResolverClient,
                    { response -> if (response.hasRegionError()) response.regionError else null },
                    { response -> if (response.hasError()) response.error else null },
                    { null },
                    startTimestamp.version,
                    false,
                )

                client.callWithRetry(
                    backOffer,
                    TikvGrpc.getKvBatchRollbackMethod(),
                    {
                        Kvrpcpb.BatchRollbackRequest.newBuilder()
                            .setContext(client.region.leaderContext)
                            .setStartVersion(startTimestamp.version)
                            .addAllKeys(regionKeys)
                            .build()
                    },
                    errorHandler,
                )
            }
        } catch (_: Exception) {
            // The remaining locks expire after LOCK_TTL_MS
        }
    }

    /**
     * Returns whether the exception, or one of its causes, reports that another transaction has written one of the
     * keys, which guarantees that this transaction has not been committed.
     */
    private fun isWriteConflict(exception: Throwable): Boolean =
        generateSequence(exception) { it.cause }.any { cause ->
            when (cause) {
                is TxnWriteConflictException, is CommonWriteConflictException -> true
                is KeyException -> cause.keyErr?.let { it.hasConflict() || it.hasAlreadyExist() }
                // The client sometimes creates the exception from the text of the key error only
                    ?: cause.message.orEmpty()
                        .let { it.contains("conflict", ignoreCase = true) || "already_exist" in it }

                else -> false
            }
        }

    /**
     * Groups the items by region and runs the action on each group, retrying with an up-to-date region when the
     * region of a group has changed.
     */
    private fun <T> forEachRegion(
        backOffer: BackOffer,
        items: List<T>,
        key: (T) -> ByteString,
        action: (TiRegion, TiStore, List<T>) -> Unit,
    ) {
        var remaining: List<T> = items

        while (remaining.isNotEmpty()) {
            val groups = remaining
                .map { item -> Pair(regionManager.getRegionStorePairByKey(key(item), backOffer), item) }
                .groupBy { (regionStore, _) -> regionStore.first.id }
                .values

            val failed = mutableListOf<T>()
            for (group in groups) {
                val region: TiRegion = group[0].first.first
                val store: TiStore = group[0].first.second
                val groupItems: List<T> = group.map { it.second }

                try {
                    action(region, store, groupItems)
                } catch (exception: RegionException) {
                    regionManager.onRequestFail(region)
                    backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, exception)
                    failed += groupItems
                } catch (exception: TiClientInternalException) {
                    regionManager.onRequestFail(region)
                    backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, exception)
                    failed += groupItems
                }
            }

            remaining = failed
        }
    }

    /**
     * Finds the first document that has been modified since it was read, defaulting to the first document.
     */
    private fun findConflict(documents: List<RawDocument>, previousValues: Map<ByteString, ByteString>): DocumentKey {
        val latestValues: Map<ByteString, ByteString> = batchGet(documents.map { it.rawKey }, session.timestamp)
        val modified: RawDocument? = documents.firstOrNull { latestValues[it.rawKey] != previousValues[it.rawKey] }

        return (modified ?: documents.first()).document.id
    }

    /**
     * A document along with its TiKV key and, if the document is updated rather than only checked, its TiKV value.
     */
    private class RawDocument(val document: Document, val rawKey: ByteString, val rawValue: ByteString?)

    private fun rawKey(document: Document): ByteString = ByteString.copyFrom(keyMapper.fromDocumentKey(document.id))

    private fun currentVersion(value: ByteString?): Long =
        if (value == null) 0 else valueMapper.toDocument(value.toByteArray()).version

    private fun mutation(op: Kvrpcpb.Op, key: ByteString, value: ByteString): Mutation =
        Mutation.newBuilder()
            .setOp(op)
            .setKey(key)
            .setValue(value)
            .build()

    //endregion

    //region getDocuments

    override fun getDocuments(ids: Iterable<DocumentKey>): Flow<Document> {
        val idList: List<DocumentKey> = ids.toList()

        return if (idList.isEmpty()) {
            emptyFlow()
        } else {
            getMultipleDocuments(idList)
        }
    }

    private fun getMultipleDocuments(idList: List<DocumentKey>): Flow<Document> = flow {
        data class RawDocumentKey(val documentKey: DocumentKey, val rawBytes: ByteString)

        val keys: List<RawDocumentKey> = idList.map {
            RawDocumentKey(it, ByteString.copyFrom(keyMapper.fromDocumentKey(it)))
        }
        val values: Map<ByteString, ByteString> = batchGet(keys.map { it.rawBytes }.distinct(), session.timestamp)

        for ((documentKey, rawBytes) in keys) {
            val value: ByteString? = values[rawBytes]

            if (value == null) {
                emit(Document(documentKey, null, 0))
            } else {
                val document: Document = valueMapper.toDocument(value.toByteArray())
                check(document.id == documentKey) {
                    "The document $documentKey has the same TiKV key as the document ${document.id}"
                }
                emit(document)
            }
        }
    }.flowOn(Dispatchers.IO)

    private fun batchGet(keys: List<ByteString>, timestamp: TiTimestamp): Map<ByteString, ByteString> =
        session.createSnapshot(timestamp)
            .batchGet(ConcreteBackOffer.BATCH_GET_MAX_BACKOFF, keys.map(ByteString::toByteArray))
            .filter { it.value.isNotEmpty() }
            .associate { pair: BytePairWrapper -> ByteString.copyFrom(pair.key) to ByteString.copyFrom(pair.value) }

    //endregion

    /**
     * Retrieves the documents of a partition, sorted by local key, whose local key is greater than or equal to
     * [startLocalKey] and lower than [endLocalKey]. When [endLocalKey] is null, all the documents of the partition
     * starting from [startLocalKey] are returned. Deleted documents are included, with a null body.
     */
    fun scan(partitionKey: String, startLocalKey: String = "", endLocalKey: String? = null): Flow<Document> = flow {
        val prefix: ByteArray = keyMapper.partitionPrefix(partitionKey)
        val startKey: ByteArray = prefix + startLocalKey.toByteArray(Charsets.UTF_8)
        val endKey: ByteArray =
            if (endLocalKey == null) prefixEnd(prefix) else prefix + endLocalKey.toByteArray(Charsets.UTF_8)

        // The iterator loads the pairs region by region, in batches
        val pairs: Iterator<Kvrpcpb.KvPair> = ConcreteScanIterator(
            session.conf,
            session.regionStoreClientBuilder,
            ByteString.copyFrom(startKey),
            ByteString.copyFrom(endKey),
            session.timestamp.version,
        )

        for (pair in pairs) {
            emit(valueMapper.toDocument(pair.value.toByteArray()))
        }
    }.flowOn(Dispatchers.IO)
}
