package org.pixode.dynadoc.tidb

private const val IS_CHECK = "is_check"

const val MAX_KEY_LENGTH = 255

class TiDbTable(table: String) {
    val tableName: String
    val updateTableName: String

    init {
        val tableNameParts: List<String> = table.split('.')

        tableName = tableNameParts.joinToString(".", transform = ::quoteIdentifier)
        updateTableName = (tableNameParts.dropLast(1) + "${tableNameParts.last()}_update")
            .joinToString(".", transform = ::quoteIdentifier)
    }

    // The collation sorts the keys by their UTF-8 encoding, without ignoring trailing spaces
    private val keyType: String = "VARCHAR($MAX_KEY_LENGTH) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"

    private val rowFilter: String =
        "t.$PARTITION_HASH = o.$PARTITION_HASH AND t.$PARTITION_KEY = o.$PARTITION_KEY AND t.$LOCAL_KEY = o.$LOCAL_KEY"

    // The scripts updating up to 9 documents, each one being created when it is first used
    private val cachedUpdateStatement: List<Lazy<String>> = List(10) { count -> lazy { createUpdateScript(count) } }

    /**
     * Creates the table, clustered by hash of the partition key, then partition key, then local key, so that the
     * partitions are spread evenly across the regions, and the documents of a partition are stored together and
     * sorted by local key.
     *
     * The `deleted` column holds the time from which a deleted document can be removed, which is done by the TTL jobs
     * of the database. It is null for the documents that are not deleted, which are never removed.
     */
    fun createTableSql(): String {
        return """
            CREATE TABLE IF NOT EXISTS $tableName (
                $PARTITION_HASH BIGINT NOT NULL,
                $PARTITION_KEY $keyType,
                $LOCAL_KEY $keyType,
                $VERSION BIGINT NOT NULL,
                $BODY JSON,
                $DELETED TIMESTAMP NULL,
                PRIMARY KEY ($PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY) CLUSTERED
            ) TTL = $DELETED + INTERVAL 0 DAY
            """.trimIndent()
    }

    /**
     * Creates the temporary table holding the documents of an update, which has the name of the table followed by
     * `_update`. Its definition is shared by all the sessions, whereas its rows are kept in the memory of the
     * database, are only visible to the transaction that inserted them, and are removed when it completes.
     */
    fun createUpdateTableSql(): String {
        return """
            CREATE GLOBAL TEMPORARY TABLE IF NOT EXISTS $updateTableName (
                i INT NOT NULL PRIMARY KEY,
                $PARTITION_HASH BIGINT NOT NULL,
                $PARTITION_KEY $keyType,
                $LOCAL_KEY $keyType,
                $VERSION BIGINT NOT NULL,
                $BODY JSON,
                $IS_CHECK BOOLEAN NOT NULL
            ) ON COMMIT DELETE ROWS
            """.trimIndent()
    }

    /**
     * Returns the script updating or checking the given number of documents. TiDB doesn't support stored procedures,
     * so the statements of the script are sent together, in a single request.
     *
     * The parameters of the script are the time, in seconds since the epoch, from which the documents deleted by the
     * update can be removed, then, for each document, the partition key, twice, the local key, the expected version,
     * the new body, and whether the version of the document is checked without the document being modified.
     *
     * The last statement of the script returns null when the documents have been updated. When a document doesn't
     * have the expected version, none of the documents is updated, and the index of that document is returned. The
     * script fails without committing its transaction, which must then be rolled back, when a document is locked or
     * created by another transaction.
     */
    fun updateScript(count: Int): String =
        cachedUpdateStatement.getOrNull(count)?.value ?: createUpdateScript(count)

    private fun createUpdateScript(count: Int): String {
        val rows: List<String> = (0 until count).map { index -> "($index, CRC32(?), ?, ?, ?, ?, ?)" }

        return """
            SET @deleted = FROM_UNIXTIME(?);

            -- Read the versions as they are once the rows have been locked
            SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

            BEGIN PESSIMISTIC;

            -- The temporary table is emptied at the end of the transaction
            INSERT INTO $updateTableName
                (i, $PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY, $VERSION, $BODY, $IS_CHECK)
            VALUES ${rows.joinToString()};

            -- Lock the documents without waiting, so that a concurrent write to them causes a conflict
            -- The hints read each row using its primary key, rather than reading the whole table
            SELECT /*+ INL_JOIN(t) */ t.$VERSION FROM $tableName t JOIN $updateTableName o ON $rowFilter
            FOR UPDATE NOWAIT;

            -- Index of the first document with an unexpected version, if any
            SET @conflict = (
                SELECT /*+ INL_JOIN(t) */ MIN(o.i) FROM $updateTableName o LEFT JOIN $tableName t ON $rowFilter
                WHERE COALESCE(t.$VERSION, 0) <> o.$VERSION
            );

            -- Fails if a document has been created by a concurrent transaction
            INSERT INTO $tableName ($PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY, $VERSION, $BODY, $DELETED)
            SELECT o.$PARTITION_HASH, o.$PARTITION_KEY, o.$LOCAL_KEY, 1, o.$BODY, IF(o.$BODY IS NULL, @deleted, NULL)
            FROM $updateTableName o
            WHERE o.$VERSION = 0 AND @conflict IS NULL;

            -- Missing checked documents were inserted above to conflict with concurrent inserts, remove them
            DELETE /*+ INL_JOIN(t) */ t FROM $tableName t JOIN $updateTableName o ON $rowFilter
            WHERE o.$IS_CHECK AND o.$VERSION = 0 AND @conflict IS NULL;

            UPDATE /*+ INL_JOIN(t) */ $tableName t JOIN $updateTableName o ON $rowFilter
            SET t.$VERSION = o.$VERSION + 1, t.$BODY = o.$BODY, t.$DELETED = IF(o.$BODY IS NULL, @deleted, NULL)
            WHERE NOT o.$IS_CHECK AND o.$VERSION > 0 AND @conflict IS NULL;

            COMMIT;

            SELECT CAST(@conflict AS SIGNED)
            """.trimIndent()
    }

    private fun quoteIdentifier(identifier: String): String = "`${identifier.replace("`", "``")}`"
}
