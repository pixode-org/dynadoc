package org.pixode.dynadoc.mongodb

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.BsonType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import org.pixode.dynadoc.core.Document
import org.pixode.dynadoc.core.DocumentKey
import org.pixode.dynadoc.core.parseDocument

private val id: DocumentKey = DocumentKey("PK", "SK")

class BsonMapperTests {
    private val now = Instant.parse("2024-01-01T20:00:00Z")
    private val bsonMapper: BsonMapper = BsonMapper(
        Duration.ofSeconds(150),
        Clock.fixed(now, ZoneId.of("UTC")),
    )

    //region fromDocument

    @ParameterizedTest
    @CsvSource(
        value = [
            """ { "key": "abc" }                    | STRING """,
            """ { "key": 999 }                      | DECIMAL128 """,
            """ { "key": -999 }                     | DECIMAL128 """,
            """ { "key": 99999999999999999999 }     | DECIMAL128 """,
            """ { "key": 1.5 }                      | DECIMAL128 """,
            """ { "key": 1e3 }                      | DECIMAL128 """,
            """ { "key": true }                     | BOOLEAN """,
            """ { "key": false }                    | BOOLEAN """,
            """ { "key": null }                     | NULL """,
            """ { "key": [ 2, 3 ] }                 | ARRAY """,
            """ { "key": { "sub": 2 } }             | DOCUMENT """,
        ],
        delimiter = '|',
    )
    fun fromDocument_validJson(json: String, expectedType: BsonType) {
        val bson: BsonDocument = fromDocument(json)

        assertEquals(3, bson.size)
        assertEquals(
            BsonDocument()
                .append(PARTITION_KEY, BsonString("PK"))
                .append(LOCAL_KEY, BsonString("SK")),
            bson[ID],
        )
        assertEquals(2L, bson[VERSION]?.asInt64()?.value)
        assertEquals(expectedType, bson["key"]?.bsonType)
    }

    @Test
    fun fromDocument_idFieldOrder() {
        val bson: BsonDocument = fromDocument(""" { "key": "abc" } """)

        assertEquals(listOf(PARTITION_KEY, LOCAL_KEY), bson.getDocument(ID).keys.toList())
    }

    @Test
    fun fromDocument_nullBody() {
        val bson: BsonDocument = fromDocument(null)

        assertEquals(3, bson.size)
        assertEquals("PK", bson.getDocument(ID).getString(PARTITION_KEY).value)
        assertEquals("SK", bson.getDocument(ID).getString(LOCAL_KEY).value)
        assertEquals(2L, bson[VERSION]?.asInt64()?.value)
        assertEquals(
            Instant.parse("2024-01-01T20:02:30Z").toEpochMilli(),
            bson[DELETED]?.asDateTime()?.value,
        )
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            """ "a" """,
            """ 10 """,
            """ true """,
            """ false """,
            """ null """,
            """ ["a"] """,
            """ a """,
        ],
    )
    fun fromDocument_invalidJsonObject(json: String) {
        val exception = assertThrows<IllegalArgumentException> {
            fromDocument(json)
        }

        assertEquals("The document must be a valid JSON object", exception.message)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            ID,
            VERSION,
            DELETED,
        ],
    )
    fun fromDocument_invalidAttributes(attribute: String) {
        val exception = assertThrows<IllegalArgumentException> {
            fromDocument(""" {"a":1,"$attribute":2} """)
        }

        assertEquals("The document cannot use the special attribute \"$attribute\"", exception.message)
    }

    //endregion fromDocument

    //region toDocument

    @ParameterizedTest
    @ValueSource(
        strings = [
            """ { "key": "abc" } """,
            """ ${"\t \n \r"} { "key": "abc" } """,
            """ { "key": 999 } """,
            """ { "key": -999 } """,
            """ { "key": 1234567890.0987654321 } """,
            """ { "key": 99999999999999999999 } """,
            """ { "key": true } """,
            """ { "key": false } """,
            """ { "key": null } """,
            """ { "key": [ 2, 3 ] } """,
            """ { "key": [ 2, "abc", { "sub": 2 } ] } """,
            """ { "key": { "sub": 2, "arr": [ 2, "abc" ] } } """,
            """ { "pk": "a", "lk": "b" } """,
            """ { } """,
        ],
    )
    fun toDocument_validJson(json: String) {
        val bson: BsonDocument = fromDocument(json)

        val document: Document = toDocument(bson)

        assertDocument(document, id, json, 2)
    }

    @Test
    fun toDocument_nullBody() {
        val bson: BsonDocument = fromDocument(null)

        val document: Document = toDocument(bson)

        assertDocument(document, id, null, 2)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            ID,
            VERSION,
        ],
    )
    fun toDocument_missingAttribute(attribute: String) {
        val bson: BsonDocument = fromDocument(""" { "key": "abc" } """).apply { remove(attribute) }

        val exception = assertThrows<NoSuchElementException> {
            toDocument(bson)
        }

        assertEquals("Key $attribute is missing in the map.", exception.message)
    }

    //endregion

    //region Helper Methods

    private fun fromDocument(body: String?) = bsonMapper.fromDocument(parseDocument(id, body, 1))

    private fun toDocument(bson: BsonDocument) = bsonMapper.toDocument(bson)

    //endregion
}
