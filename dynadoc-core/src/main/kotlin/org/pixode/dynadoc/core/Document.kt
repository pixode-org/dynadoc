package org.pixode.dynadoc.core

/**
 * Represents a document composed of a unique ID, a JSON body and a version number.
 */
data class Document(
    /** The unique identifier of the document. **/
    val id: DocumentKey,

    /** The JSON body of the document as a string, or null if the document does not exist. **/
    val body: String?,

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
