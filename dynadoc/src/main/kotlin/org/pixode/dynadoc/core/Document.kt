package org.pixode.dynadoc.core

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement

/**
 * Represents a document composed of a unique ID, a JSON body and a version number.
 */
data class Document(
    /** The unique identifier of the document. **/
    val id: DocumentKey,

    /** The JSON body of the document, or null if the document does not exist. **/
    val body: JsonElement?,

    /** The current version of the document. **/
    val version: Long,
)

/**
 * Represents a key uniquely identifying a document in a store.
 */
data class DocumentKey(
    val partitionKey: String,
    val sortKey: String,
) {
    override fun toString() = "(\"$partitionKey\", \"$sortKey\")"
}


fun parseDocument(id: DocumentKey, body: String?, version: Long) = Document(
    id = id,
    body = body?.let { Json.parseToJsonElement(it) },
    version = version,
)
