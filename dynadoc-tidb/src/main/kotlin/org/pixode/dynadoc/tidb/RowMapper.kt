package org.pixode.dynadoc.tidb

import io.r2dbc.spi.Readable
import kotlinx.serialization.json.Json
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey

const val PARTITION_HASH = "partition_hash"
const val PARTITION_KEY = "partition_key"
const val LOCAL_KEY = "local_key"
const val VERSION = "version"
const val BODY = "body"
const val DELETED = "deleted"

/**
 * Maps documents to the rows of a TiDB table.
 *
 * A row has the columns `partition_hash`, `partition_key`, `local_key`, `version` and `body`. The body is a JSON
 * column, exchanged with the database as text so that the mapping doesn't depend on a specific driver. A deleted
 * document is stored with a null body.
 */
object RowMapper {
    fun toDocument(row: Readable): Document = Document(
        id = toDocumentKey(row),
        body = row.get(BODY, String::class.java)?.let { Json.parseToJsonElement(it) },
        version = toVersion(row),
    )

    fun toVersion(row: Readable): Long = checkNotNull(row.get(VERSION, Long::class.javaObjectType))

    fun toDocumentKey(row: Readable): DocumentKey = DocumentKey(
        partitionKey = checkNotNull(row.get(PARTITION_KEY, String::class.java)),
        localKey = checkNotNull(row.get(LOCAL_KEY, String::class.java)),
    )
}
