package org.pixode.dynadoc.core

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first

/**
 * Represents a service object used to retrieve and modify documents.
 */
interface DocumentStore {
    /**
     * Updates atomically the body of multiple documents.
     */
    @Throws(UpdateConflictException::class)
    suspend fun updateDocuments(updatedDocuments: Iterable<Document>, checkedDocuments: Iterable<Document>)

    /**
     * Retrieves multiple documents given their IDs.
     */
    fun getDocuments(ids: Iterable<DocumentKey>): Flow<Document>
}

/**
 * Represents an error that occurs when attempting to modify a document using the wrong base version.
 */
class UpdateConflictException(
    val id: DocumentKey,
) : RuntimeException("The object $id has been modified.")


@Throws(UpdateConflictException::class)
suspend fun DocumentStore.updateDocuments(vararg documents: Document) =
    updateDocuments(documents.toList(), emptyList())

suspend fun DocumentStore.getDocument(id: DocumentKey): Document =
    getDocuments(listOf(id)).first()
