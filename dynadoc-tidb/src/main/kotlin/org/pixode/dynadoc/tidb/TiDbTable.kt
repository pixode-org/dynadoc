package org.pixode.dynadoc.tidb

import java.util.zip.CRC32

const val MAX_KEY_LENGTH = 255

class TiDbTable(table: String) {
    val tableName: String = table.split('.').joinToString(".", transform = ::quoteIdentifier)

    /**
     * The statement reading the documents, to which a `WHERE` clause may be appended.
     */
    val selectSql: String =
        "SELECT $PARTITION_KEY, $SORT_KEY, $VERSION, CAST($BODY AS CHAR) AS $BODY FROM $tableName"

    /**
     * The statement creating the table.
     */
    val createTableSql = """
        CREATE TABLE IF NOT EXISTS $tableName (
            $PARTITION_HASH BIGINT NOT NULL,
            $PARTITION_KEY VARCHAR($MAX_KEY_LENGTH) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
            $SORT_KEY VARCHAR($MAX_KEY_LENGTH) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
            $VERSION BIGINT NOT NULL,
            $BODY JSON,
            $DELETED TIMESTAMP NULL,
            PRIMARY KEY ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY) CLUSTERED
        ) TTL = $DELETED + INTERVAL 0 DAY
    """.trimIndent()

    /**
     * The statement reading the specified documents, using a single batch read.
     */
    val selectDocumentsSql: (Int) -> String = cached { count ->
        "$selectSql WHERE ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY) IN (${rows("(?, ?, ?)", count)})"
    }

    /**
     * The statement reading the versions of the specified documents for update. It will make the commit fail if
     * any of them has been updated by a concurrent transaction.
     */
    val selectVersionsSql: (Int) -> String = cached { count ->
        """
            SELECT $PARTITION_KEY, $SORT_KEY, $VERSION FROM $tableName
            WHERE ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY) IN (${rows("(?, ?, ?)", count)}) FOR UPDATE
        """.trimIndent()
    }

    /**
     * The statement updating a single document. It doesn't update any row if the document doesn't have the expected
     * version.
     */
    val updateSingleSql: String = """
        UPDATE $tableName SET $VERSION = $VERSION + 1, $BODY = ?, $DELETED = FROM_UNIXTIME(?)
        WHERE $PARTITION_HASH = ? AND $PARTITION_KEY = ? AND $SORT_KEY = ? AND $VERSION + 1 = ?
    """.trimIndent()

    /**
     * The statement updating the specified documents. The documents must exist, and their versions must have already
     * been checked before the statement is executed.
     */
    val updateSql: (Int) -> String = cached { count ->
        """
            INSERT INTO $tableName ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY, $VERSION, $BODY, $DELETED)
            VALUES ${rows("(?, ?, ?, ?, ?, FROM_UNIXTIME(?))", count)}
            ON DUPLICATE KEY UPDATE $VERSION = VALUES($VERSION), $BODY = VALUES($BODY), $DELETED = VALUES($DELETED)
        """.trimIndent()
    }

    /**
     * The statement creating the specified documents. It fails if any key already exists, whether it is the primary
     * key or another unique index.
     */
    val insertSql: (Int) -> String = cached { count ->
        """
            INSERT INTO $tableName ($PARTITION_HASH, $PARTITION_KEY, $SORT_KEY, $VERSION, $BODY, $DELETED)
            VALUES ${rows("(?, ?, ?, ?, ?, FROM_UNIXTIME(?))", count)}
        """.trimIndent()
    }
    
    // The rows of a statement are separated by commas
    private fun rows(row: String, count: Int): String =
        generateSequence { row }.take(count).joinToString(separator = ", ")

    private fun cached(create: (Int) -> String): (Int) -> String {
        val statements: List<Lazy<String>> = List(10) { count -> lazy { create(count) } }
        return { count ->
            if (count < statements.size) {
                statements[count].value
            } else {
                create(count)
            }
        }
    }

    private fun quoteIdentifier(identifier: String): String = "`${identifier.replace("`", "``")}`"
}

/**
 * Returns the value of the `partition_hash` column for the given partition key.
 */
fun partitionHash(partitionKey: String): Long {
    val bytes: ByteArray = partitionKey.toByteArray()
    return CRC32().apply { update(bytes, 0, bytes.size) }.value
}
