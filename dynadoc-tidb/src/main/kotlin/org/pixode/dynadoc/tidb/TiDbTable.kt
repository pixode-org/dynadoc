package org.pixode.dynadoc.tidb

// Sorts the keys by their UTF-8 encoding, without ignoring trailing spaces
private const val KEY_COLLATION = "utf8mb4_0900_bin"
// The user variable holding the result of the update script
private const val CONFLICT = "@dynadoc_conflict"
// Reads each row using its primary key, rather than reading the whole table
private const val ROW_LOOKUP = "/*+ INL_JOIN(t) */"
private const val INDEX = "i"
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

    private val keyType: String = "VARCHAR($MAX_KEY_LENGTH) CHARACTER SET utf8mb4 COLLATE $KEY_COLLATION NOT NULL"

    private val rowFilter: String =
        "t.$PARTITION_HASH = o.$PARTITION_HASH AND t.$PARTITION_KEY = o.$PARTITION_KEY AND t.$LOCAL_KEY = o.$LOCAL_KEY"

    /**
     * Creates the table, clustered by hash of the partition key, then partition key, then local key, so that the
     * partitions are spread evenly across the regions, and the documents of a partition are stored together and
     * sorted by local key.
     */
    fun createTableSql(): String {
        return """
            CREATE TABLE IF NOT EXISTS $tableName (
                $PARTITION_HASH BIGINT NOT NULL,
                $PARTITION_KEY $keyType,
                $LOCAL_KEY $keyType,
                $VERSION BIGINT NOT NULL,
                $BODY JSON,
                PRIMARY KEY ($PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY) CLUSTERED
            )
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
                $INDEX INT NOT NULL PRIMARY KEY,
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
     * Returns the script updating the given numbers of updated and checked documents. TiDB doesn't support stored
     * procedures, so the statements of the script are sent together, in a single request.
     *
     * The parameters of the script are the partition key, twice, the local key, the expected version and the new body
     * of each updated document, followed by the partition key, twice, the local key and the expected version of each
     * checked document.
     *
     * The last statement of the script returns null when the documents have been updated. When a document doesn't
     * have the expected version, none of the documents is updated, and the index of that document is returned. The
     * script fails without committing its transaction, which must then be rolled back, when a document is locked or
     * created by another transaction.
     */
    fun updateScript(updatedCount: Int, checkedCount: Int): String {
        val rows: List<String> = (0 until updatedCount + checkedCount).map { index ->
            if (index < updatedCount) {
                "($index, CRC32(?), ?, ?, ?, ?, FALSE)"
            } else {
                "($index, CRC32(?), ?, ?, ?, NULL, TRUE)"
            }
        }

        val statements: List<String> = listOf(
            // The versions are checked after the rows have been locked, so they must be read as they are then
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "BEGIN PESSIMISTIC",
            // The temporary table is emptied at the end of the transaction
            "INSERT INTO $updateTableName " +
                "($INDEX, $PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY, $VERSION, $BODY, $IS_CHECK) " +
                "VALUES ${rows.joinToString()}",
            // Lock the documents without waiting, so that a concurrent write to them causes a conflict
            """
            SELECT $ROW_LOOKUP t.$VERSION FROM $tableName t JOIN $updateTableName o ON $rowFilter
            FOR UPDATE NOWAIT
            """.trimIndent(),
            """
            SET $CONFLICT = (
                SELECT $ROW_LOOKUP MIN(o.$INDEX) FROM $updateTableName o LEFT JOIN $tableName t ON $rowFilter
                WHERE COALESCE(t.$VERSION, 0) <> o.$VERSION
            )
            """.trimIndent(),
            // Fails if a document has been created by a concurrent transaction
            """
            INSERT INTO $tableName ($PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY, $VERSION, $BODY)
            SELECT o.$PARTITION_HASH, o.$PARTITION_KEY, o.$LOCAL_KEY, 1, o.$BODY FROM $updateTableName o
            WHERE o.$VERSION = 0 AND $CONFLICT IS NULL
            """.trimIndent(),
            // A checked document that doesn't exist is inserted then deleted, so that a concurrent insert of the same
            // document causes a conflict
            """
            DELETE $ROW_LOOKUP t FROM $tableName t JOIN $updateTableName o ON $rowFilter
            WHERE o.$IS_CHECK AND o.$VERSION = 0 AND $CONFLICT IS NULL
            """.trimIndent(),
            """
            UPDATE $ROW_LOOKUP $tableName t JOIN $updateTableName o ON $rowFilter
            SET t.$VERSION = o.$VERSION + 1, t.$BODY = o.$BODY
            WHERE NOT o.$IS_CHECK AND o.$VERSION > 0 AND $CONFLICT IS NULL
            """.trimIndent(),
            "COMMIT",
            "SELECT CAST($CONFLICT AS SIGNED)",
        )

        return statements.joinToString(";\n")
    }

    private fun quoteIdentifier(identifier: String): String = "`${identifier.replace("`", "``")}`"
}
