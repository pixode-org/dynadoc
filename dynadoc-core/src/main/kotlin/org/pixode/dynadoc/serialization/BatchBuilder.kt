package org.pixode.dynadoc.serialization

import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.UpdateConflictException

class BatchBuilder(
    private val store: EntityStore,
) {
    private val checkedDocuments = mutableMapOf<DocumentKey, JsonEntity<Any?>>()
    private val modifiedDocuments = mutableMapOf<DocumentKey, JsonEntity<Any?>>()

    fun modify(vararg documents: JsonEntity<Any?>) {
        val removeCheckedDocuments = mutableSetOf<DocumentKey>()
        val addModifyDocuments = mutableListOf<JsonEntity<Any?>>()

        // Validation phase
        for (document in documents) {
            val existingCheckedDocument = checkedDocuments[document.id]

            if (existingCheckedDocument != null) {
                if (existingCheckedDocument.version == document.version) {
                    removeCheckedDocuments += document.id
                } else {
                    error("A different version of document ${document.id} is already being checked.")
                }

            } else if (document.id in modifiedDocuments) {
                error("Document ${document.id} is already being modified.")
            }

            addModifyDocuments += document
        }

        checkedDocuments -= removeCheckedDocuments
        modifiedDocuments += addModifyDocuments.associateBy { it.id }
    }

    fun check(vararg documents: JsonEntity<Any?>) {
        val addCheckedDocuments = mutableListOf<JsonEntity<Any?>>()

        for (document in documents)
        {
            val existingCheckedDocument = checkedDocuments[document.id]
            val existingModifiedDocument = modifiedDocuments[document.id]

            if (existingCheckedDocument != null) {
                if (existingCheckedDocument.version == document.version) {
                    continue
                } else {
                    error("A different version of document ${document.id} is already being checked.")
                }

            } else if (existingModifiedDocument != null) {
                if (existingModifiedDocument.version == document.version) {
                    continue
                } else {
                    error("A different version of document ${document.id} is already being modified.")
                }
            }

            addCheckedDocuments += document;
        }

        checkedDocuments += addCheckedDocuments.associateBy { it.id }
    }

    @Throws(UpdateConflictException::class)
    suspend fun submit() {
        store.updateEntities(modifiedDocuments.values, checkedDocuments.values)

        checkedDocuments.clear()
        modifiedDocuments.clear()
    }
}


fun <T, U> BatchBuilder.modify(entity: JsonEntity<T>, builder: T.() -> U) = modify(entity.modify(builder))
