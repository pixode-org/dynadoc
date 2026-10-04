package org.pixode.dynadoc.yugabytedb

// The error raised by the update function to roll back its changes when a document has been modified
private const val UPDATE_CONFLICT = "DD001"
private const val HASH_COUNT = 65536
private const val TABLET_COUNT = 256

class YugabyteDbTable(table: String) {
    val tableName: String
    val updateFunction: String

    init {
        val tableNameParts: List<String> = table.split('.')

        tableName = tableNameParts.joinToString(".", transform = ::quoteIdentifier)
        updateFunction =
            (tableNameParts.dropLast(1) + "${tableNameParts.last()}_update").joinToString(".", transform = ::quoteIdentifier)
    }

    /**
     * Creates the table, range-sharded by hash of the partition key, then partition key, then local key, so that the
     * partitions are spread evenly across the tablets, and the documents of a partition are stored together and
     * sorted by local key.
     *
     * The table is initially split into 256 tablets, each holding an equal share of the hashes.
     * Tablets are then split automatically as they grow, including within a partition.
     *
     * The function used to update the documents of the table is created along with the table, or replaced if it
     * already exists. It has the name of the table followed by `_update`.
     */
    fun createTableSql(): String {
        return """
            CREATE TABLE IF NOT EXISTS $tableName (
                $PARTITION_HASH INT NOT NULL,
                $PARTITION_KEY TEXT COLLATE "C" NOT NULL,
                $LOCAL_KEY TEXT COLLATE "C" NOT NULL,
                $VERSION BIGINT NOT NULL,
                $BODY JSONB,
                PRIMARY KEY ($PARTITION_HASH ASC, $PARTITION_KEY ASC, $LOCAL_KEY ASC),
                CHECK ($PARTITION_HASH = yb_hash_code($PARTITION_KEY))
            ) SPLIT AT VALUES (${(1 until TABLET_COUNT).joinToString { i -> "(${HASH_COUNT * i / TABLET_COUNT})" }})
            """.trimIndent()
    }

    /**
     * Returns the definition of the function updating the documents described by its parameter (see
     * [RowMapper.fromDocuments]).
     *
     * The function returns -1 when the documents have been updated. When a document doesn't have the expected version
     * or is locked by another transaction, none of the documents is updated, and the function returns the index of
     * that document. The function runs in the transaction of the statement calling it, so the update is atomic.
     */
    fun createFunctionSql(): String {
        val rowFilter =
            "$PARTITION_HASH = v_partition_hash AND $PARTITION_KEY = v_partition_key AND $LOCAL_KEY = v_local_key"

        return """
            CREATE OR REPLACE FUNCTION $updateFunction(p_operations JSONB) RETURNS INT
            LANGUAGE plpgsql AS $$
            DECLARE
                v_index INT := 0;
                v_operation JSONB;
                v_partition_hash INT;
                v_partition_key TEXT;
                v_local_key TEXT;
                v_check BOOLEAN;
                v_body JSONB;
                v_expected_version BIGINT;
                v_current_version BIGINT;
                v_rows INT;
            BEGIN
                -- The changes made in this block are rolled back when the block fails
                BEGIN
                    FOR v_operation IN
                        SELECT value FROM jsonb_array_elements(p_operations) WITH ORDINALITY ORDER BY ordinality
                    LOOP
                        v_partition_key := v_operation->>'$PARTITION_KEY';
                        v_local_key := v_operation->>'$LOCAL_KEY';
                        v_partition_hash := yb_hash_code(v_partition_key);
                        v_check := (v_operation->>'$CHECK')::BOOLEAN;
                        v_body := NULLIF(v_operation->'$BODY', 'null'::JSONB);
                        v_expected_version := (v_operation->>'$VERSION')::BIGINT;
                        v_current_version := NULL;

                        -- Lock the document without waiting, so that a concurrent write to it causes a conflict
                        SELECT $VERSION INTO v_current_version FROM $tableName WHERE $rowFilter FOR UPDATE NOWAIT;

                        IF COALESCE(v_current_version, 0) <> v_expected_version THEN
                            RAISE EXCEPTION USING ERRCODE = '$UPDATE_CONFLICT';
                        END IF;

                        IF v_current_version IS NULL THEN
                            INSERT INTO $tableName ($PARTITION_HASH, $PARTITION_KEY, $LOCAL_KEY, $VERSION, $BODY)
                            VALUES (v_partition_hash, v_partition_key, v_local_key, 1, v_body)
                            ON CONFLICT DO NOTHING;

                            GET DIAGNOSTICS v_rows = ROW_COUNT;
                            IF v_rows = 0 THEN
                                RAISE EXCEPTION USING ERRCODE = '$UPDATE_CONFLICT';
                            END IF;

                            -- A checked document that doesn't exist is inserted then deleted, so that a concurrent
                            -- insert of the same document causes a conflict
                            IF v_check THEN
                                DELETE FROM $tableName WHERE $rowFilter;
                            END IF;
                        ELSIF NOT v_check THEN
                            UPDATE $tableName SET $VERSION = v_expected_version + 1, $BODY = v_body WHERE $rowFilter;
                        END IF;

                        v_index := v_index + 1;
                    END LOOP;
                EXCEPTION WHEN SQLSTATE '$UPDATE_CONFLICT' OR lock_not_available OR unique_violation THEN
                    RETURN v_index;
                END;

                RETURN -1;
            END
            $$
            """.trimIndent()
    }

    private fun quoteIdentifier(identifier: String): String = "\"${identifier.replace("\"", "\"\"")}\""
}