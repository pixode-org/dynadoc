package org.pixode.dynadoc.tidb

import java.util.zip.CRC32

const val MAX_KEY_LENGTH = 255

// The statements reading and writing up to 9 documents are created once
private const val CACHED_COUNT = 10

class TiDbTable(table: String) {
    val tableName: String = table.split('.').joinToString(".", transform = ::quoteIdentifier)

    // The collation sorts the keys by their UTF-8 encoding, without ignoring trailing spaces
    private val keyType: String = "VARCHAR($MAX_KEY_LENGTH) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"

    private val cachedSelectDocumentsSql = cached(::createSelectDocumentsSql)
    private val cachedSelectVersionsSql = cached(::createSelectVersionsSql)
    private val cachedUpsertSql = cached(::createUpsertSql)
    private val cachedDeleteSql = cached(::createDeleteSql)

    /**
     * The statement reading the documents, to which a condition is appended.
     */
    val selectSql: String =
        "SELECT $PARTITION_KEY, $SORT_KEY, $VERSION, CAST($BODY AS CHAR) AS $BODY FROM $tableName"

    /**
     * Creates the table, clustered by hash of the partition key, then partition key, then sort key, so that the
     * partitions are spread evenly across the regions, and the documents of a partition are stored together and
     * sorted by sort key.
     *
     * The `deleted` column holds the time from which a deleted document can be removed, which is done by the TTL jobs
     * of the database. It is null for the documents that are not deleted, which are never removed.
     */
    val createTableSql = """
        CREATE TABLE IF NOT EXISTS $tableName (
            $PARTITION_HASH BIGINT NOT NULL,
            $PARTITION_KEY $keyType,
            $SORT_KEY $keyType,
            $VERSION BIGINT NOT NULL,
            $BODY JSON,
            $DELETED TIMESTAMP NULL,
            PRIMARY KEY ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY) CLUSTERED
        ) TTL = $DELETED + INTERVAL 0 DAY
    """.trimIndent()

    /**
     * Returns the statement creating a document, which fails if the document already exists. Its parameters are the
     * partition hash, the partition key, the sort key, the body, and the time, in seconds since the epoch, from which
     * the document can be removed if it is deleted.
     */
    val insertSql: String = """
        INSERT INTO $tableName ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY, $VERSION, $BODY, $DELETED)
        VALUES (?, ?, ?, 1, ?, FROM_UNIXTIME(?))
    """.trimIndent()

    /**
     * Returns the statement updating a document, which doesn't update any row if the document doesn't have the
     * expected version. Its parameters are the body, the time, in seconds since the epoch, from which the document
     * can be removed if it is deleted, the partition hash, the partition key, the sort key and the expected version.
     */
    val updateSql: String = """
        UPDATE $tableName SET $VERSION = $VERSION + 1, $BODY = ?, $DELETED = FROM_UNIXTIME(?)
        WHERE $PARTITION_HASH = ? AND $PARTITION_KEY = ? AND $SORT_KEY = ? AND $VERSION = ?
    """.trimIndent()

    /**
     * Returns the statement reading the given number of documents, using a single batch read. Its parameters are,
     * for each document, the partition hash, the partition key and the sort key.
     */
    fun selectDocumentsSql(count: Int): String = cachedSelectDocumentsSql(count)

    private fun createSelectDocumentsSql(count: Int): String =
        "$selectSql WHERE ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY) IN (${keyTuples(count)})"

    /**
     * Returns the statement reading the key and the version of the given number of documents, which exist, within a
     * transaction. In an optimistic transaction, reading the documents for update doesn't lock them, but makes the
     * commit fail if any of them has been written by a concurrent transaction since the transaction began.
     *
     * The parameters of the statement are, for each document, the partition hash, the partition key and the sort key.
     */
    fun selectVersionsSql(count: Int): String = cachedSelectVersionsSql(count)

    private fun createSelectVersionsSql(count: Int): String = """
        SELECT $PARTITION_KEY, $SORT_KEY, $VERSION FROM $tableName
        WHERE ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY) IN (${keyTuples(count)}) FOR UPDATE
    """.trimIndent()

    /**
     * Returns the statement writing the given number of documents, which are created if they don't exist. Its
     * parameters are, for each document, the partition hash, the partition key, the sort key, the current version, the
     * new body, and the time, in seconds since the epoch, from which the document can be removed if it is deleted.
     *
     * The versions of the documents must have been checked before the statement is executed.
     */
    fun upsertSql(count: Int): String = cachedUpsertSql(count)

    private fun createUpsertSql(count: Int): String = """
        INSERT INTO $tableName ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY, $VERSION, $BODY, $DELETED)
        VALUES ${rows("(?, ?, ?, ? + 1, ?, FROM_UNIXTIME(?))", count)}
        ON DUPLICATE KEY UPDATE $VERSION = VALUES($VERSION), $BODY = VALUES($BODY), $DELETED = VALUES($DELETED)
    """.trimIndent()

    /**
     * Returns the statement deleting the given number of documents. The parameters of the statement are, for each
     * document, the partition hash, the partition key and the sort key.
     */
    fun deleteSql(count: Int): String = cachedDeleteSql(count)

    private fun createDeleteSql(count: Int): String = """
        DELETE FROM $tableName WHERE ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY) IN (${keyTuples(count)})
    """.trimIndent()

    private fun keyTuples(count: Int): String = rows("(?, ?, ?)", count)

    // The rows of a statement are separated by commas
    private fun rows(row: String, count: Int): String = generateSequence { row }.take(count).joinToString()

    private fun cached(create: (Int) -> String): (Int) -> String {
        val statements: List<Lazy<String>> = List(CACHED_COUNT) { count -> lazy { create(count) } }

        return { count -> statements.getOrNull(count)?.value ?: create(count) }
    }

    private fun quoteIdentifier(identifier: String): String = "`${identifier.replace("`", "``")}`"
}

/**
 * Returns the value of the `partition_hash` column for the given partition key, which is the same as the `CRC32`
 * function of TiDB. It is passed to the statements as a parameter rather than computed by the database, because TiDB
 * doesn't cache the plans of the statements reading several documents when the hash is computed from a parameter.
 */
fun partitionHash(partitionKey: String): Long {
    val bytes: ByteArray = partitionKey.toByteArray()
    return CRC32().apply { update(bytes, 0, bytes.size) }.value
}
