package org.pixode.dynadoc.yugabytedb

import io.r2dbc.spi.Readable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey

const val PARTITION_HASH = "partition_hash"
const val PARTITION_KEY = "partition_key"
const val SORT_KEY = "sort_key"
const val VERSION = "version"
const val BODY = "body"
const val CHECK = "check"

/**
 * Maps documents to the rows of a YugabyteDB table, and to the parameter of the function updating them.
 *
 * A row has the columns `partition_key`, `sort_key`, `version` and `body`. The body is a JSONB column, exchanged with
 * the database as text so that the mapping doesn't depend on a specific driver. A deleted document is stored with a
 * null body.
 */
object RowMapper {
    fun toDocument(row: Readable): Document = Document(
        id = toDocumentKey(row),
        body = row.get(BODY, String::class.java)?.let { Json.parseToJsonElement(it) },
        version = toVersion(row),
    )

    fun toDocumentKey(row: Readable): DocumentKey = DocumentKey(
        partitionKey = checkNotNull(row.get(PARTITION_KEY, String::class.java)),
        sortKey = checkNotNull(row.get(SORT_KEY, String::class.java)),
    )

    fun toVersion(row: Readable): Long = checkNotNull(row.get(VERSION, Long::class.javaObjectType))

    /**
     * Returns the parameter of the update function, which is a JSON array with an element of the form
     * `{ partition_key, sort_key, version, body, check }` for each document, where `version` is the expected version
     * of the document, `body` is the new body of the document, or null if the document is deleted, and `check`
     * indicates that the version of the document is checked without the document being modified.
     */
    fun fromDocuments(updatedDocuments: List<Document>, checkedDocuments: List<Document>): String {
        val operations: List<JsonObject> =
            updatedDocuments.map { fromDocument(it, it.body, false) } +
                checkedDocuments.map { fromDocument(it, null, true) }

        return JsonArray(operations).toString()
    }

    private fun fromDocument(document: Document, body: JsonElement?, check: Boolean): JsonObject {
        require(body == null || body is JsonObject) {
            "The document must be a valid JSON object"
        }

        return JsonObject(
            mapOf(
                PARTITION_KEY to JsonPrimitive(document.id.partitionKey),
                SORT_KEY to JsonPrimitive(document.id.sortKey),
                VERSION to JsonPrimitive(document.version),
                BODY to (body ?: JsonNull),
                CHECK to JsonPrimitive(check),
            )
        )
    }
}
