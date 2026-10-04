package org.pixode.dynadoc.yugabytedb

import io.r2dbc.spi.Readable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey

const val PARTITION_HASH = "partition_hash"
const val PARTITION_KEY = "partition_key"
const val LOCAL_KEY = "local_key"
const val VERSION = "version"
const val BODY = "body"

/**
 * Maps documents to the rows of a YugabyteDB table.
 *
 * A row has the columns `partition_key`, `local_key`, `version` and `body`. The body is a JSONB column, exchanged with
 * the database as text so that the mapping doesn't depend on a specific driver. A deleted document is stored with a
 * null body.
 */
class RowMapper {
    fun toDocument(row: Readable): Document = Document(
        id = toDocumentKey(row),
        body = row.get(BODY, String::class.java)?.let { Json.parseToJsonElement(it) },
        version = toVersion(row),
    )

    fun toDocumentKey(row: Readable): DocumentKey = DocumentKey(
        partitionKey = checkNotNull(row.get(PARTITION_KEY, String::class.java)),
        localKey = checkNotNull(row.get(LOCAL_KEY, String::class.java)),
    )

    fun toVersion(row: Readable): Long = checkNotNull(row.get(VERSION, Long::class.javaObjectType))

    /**
     * Returns the value of the `body` column for the document, or null if the document is deleted.
     */
    fun fromBody(document: Document): String? {
        val body: JsonElement? = document.body
        require(body == null || body is JsonObject) {
            "The document must be a valid JSON object"
        }

        return body?.toString()
    }
}
