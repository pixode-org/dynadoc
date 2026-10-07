# Dynadoc

<a href="https://central.sonatype.com/artifact/org.pixode/dynadoc">![Maven Central Version](https://img.shields.io/maven-central/v/org.pixode/dynadoc)</a>

Dynadoc is a Kotlin library for using DynamoDB or TiDB as a JSON document store. It manages the mapping between Kotlin objects and JSON documents.

## Concepts

### DynamoDB mapping

Dynadoc translates JSON documents to DynamoDB items by converting top-level keys into DynamoDB attributes.

It also adds a few special attributes that don't appear in the JSON, but appear in the `JsonEntity` objects:

- `partition_key`: The partition key of the DynamoDB table. 
- `sort_key`: The sort key of the document, used as the sort key of the DynamoDB table.
- `version`: An integer representing the version of the item, for optimistic concurrency management purposes.
- `deleted`: An attribute set on deleted objects. It contains a value that can be used with the TTL feature of DynamoDB to clear soft-deleted items from the table.

### TiDB mapping

With TiDB, each document is stored as a row of a table with the following schema:

```sql
CREATE TABLE documents (
    partition_hash BIGINT NOT NULL,
    partition_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    sort_key VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
    version BIGINT NOT NULL,
    body JSON,
    deleted TIMESTAMP NULL,
    PRIMARY KEY (partition_hash, partition_key, sort_key) CLUSTERED
) TTL = deleted + INTERVAL 0 DAY;
```

- `partition_hash` holds the hash of the partition key, which is the same as the `CRC32` function of TiDB computes. It is set by the store when a document is written.
- `partition_key` and `sort_key` hold the partition key and the sort key, which are limited to 255 characters. The `utf8mb4_0900_bin` collation sorts keys by their UTF-8 encoding, and doesn't ignore trailing spaces.
- `version` is an integer representing the version of the document, for optimistic concurrency management purposes.
- `body` holds the JSON document. Since the body has its own column, there are no reserved field names.
- `deleted` is set on deleted documents. It holds the time from which the document can be removed, and is used by the [TTL feature](https://docs.pingcap.com/tidb/stable/time-to-live) of TiDB to clear soft-deleted documents from the table.

The primary key is clustered and starts with the hash of the partition key. Partitions are therefore spread evenly across the regions, even when partition keys are increasing values such as timestamps or sequence numbers. The documents of a partition are stored together and sorted by sort key.

The `JSON` type of TiDB stores numbers that are not integers as double-precision floating-point numbers, so a number such as `1234567890.0987654321` is read back as `1234567890.0987654`.

Deleted documents are kept with a `NULL` body so that their version is preserved, until TiDB removes them.

### The `JsonEntity<T>` type

In the application code, documents are represented using the `JsonEntity<T>` type:

```kotlin
data class JsonEntity<out T>(
    /** The unique identifier of the document. **/
    val id: DocumentKey,

    /** The body of the document deserialized into an object,
     * or null if the document does not exist. **/
    val entity: T,

    /** The current version of the document. **/
    val version: Long,
)
```

The `Entity` property can be null if the document does not exist. This can be the case for a document that hasn't been created yet, or for a document that has been deleted.

`DocumentKey` is defined as follows:

```kotlin
data class DocumentKey(
    val partitionKey: String,
    val sortKey: String,
)
```

## Setup and configuration

### Packages

A dependency to [dynadoc-dynamodb](https://central.sonatype.com/artifact/org.pixode/dynadoc-dynamodb) should be added to the project. It transitively references the core [dynadoc](https://central.sonatype.com/artifact/org.pixode/dynadoc) library and the AWS SDK for DynamoDB:

```kotlin
dependencies {
    implementation("org.pixode:dynadoc-dynamodb:VERSION")
}
```

The core `dynadoc` library does not depend on the AWS SDK. `DynamoDbDocumentStore` is in the `org.pixode.dynadoc.dynamodb` package.

To use TiDB instead, add a dependency to `dynadoc-tidb`, which references the R2DBC SPI. `TiDbDocumentStore` is in the `org.pixode.dynadoc.tidb` package. See [Using TiDB](#using-tidb).

**Note:** The [dynadoc-jackson](https://central.sonatype.com/artifact/org.pixode/dynadoc-jackson) library is also provided to allow using Jackson as the JSON serializer instead of kotlinx.serialization. It is not required to use the core library, and can be added as an optional dependency.

### Initialization

Dynadoc requires an instance of a `DynamoDbClient` object to construct the base `DynamoDbDocumentStore` object.

```kotlin
val client: DynamoDbClient = DynamoDbClient {
    credentialsProvider = DefaultChainCredentialsProvider()
}

val documentStore: DynamoDbDocumentStore = DynamoDbDocumentStore(client, "tablename")
```

Then, an `EntityStore` object should be instantiated:

```kotlin
val entityStore: EntityStore = EntityStore(documentStore, DefaultJsonSerializer)
```

The `DefaultJsonSerializer` singleton relies on the default `Json` object, but it is possible to create an instance of the `KotlinJsonSerializer` and provide a custom `Json` object to customize the serializer settings. 

### Usage with dependency injection

When using a dependency injection framework such as Guice, a factory function such as this can be used:

```kotlin
@Provides
@Singleton
fun entityStore(awsCredentialsProvider: CredentialsProvider): EntityStore {
    val client = DynamoDbClient {
        credentialsProvider = awsCredentialsProvider
    }

    val documentStore = DynamoDbDocumentStore(client, "tablename")
    
    return EntityStore(documentStore, DefaultJsonSerializer)
}
```

The `EntityStore` and `DynamoDbDocumentStore` classes are thread-safe, and can be used as singletons.

## Defining document types

Document types can be any class serializable to JSON.

Here is an example of a document type:

```kotlin
@Serializable
data class Product(
    val name: String,
    val aisle: Int,
    val price: Double,
    val stockQuantity: Int,
    val categories: List<String>
)
```

### Creating a new document

In order to add a new document to the store, a `JsonEntity` object representing the data to insert should first be created.

```kotlin
val product = Product(
    name = "Vanilla Ice Cream",
    aisle = 3,
    price = 9.95,
    stockQuantity = 140,
    categories = listOf("Frozen Foods", "Organic")
)

val entity: JsonEntity<Product> = createEntity(
    partitionKey = "vanilla-ice-cream",
    sortKey = "product",
    entity = product
)
```

Then, the `updateEntities` method on the `EntityStore` class is used to commit the document in the document store.

```kotlin
entityStore.updateEntities(entity)
```

## Retrieving a document by ID

The simplest way to retrieve a document is by using its ID, with the `getEntity` method.

```kotlin
// The ID of the document is already known
val key: DocumentKey

val entity: JsonEntity<Product?> = entityStore.getEntity(key)
```

If the document does not exist, this method will return a "shadow" `JsonEntity<T>` object with a `null` body and a version number of `0`. It is possible to update this "shadow" document the same way a normal document can be updated, which will result in the document being effectively created in the table.

It is possible to ensure the document exists by using the `ifExists` function.

```kotlin
val existingEntity: JsonEntity<Product> = entity.ifExists() ?: error("The entity was not found.")
```

## Modifying a document

Dynadoc relies on the read-modify-write pattern, with mandatory optimistic concurrency control to ensure safe writes.

The entity to modify should first be read from the data store, either by using its ID as seen above, or using custom queries as seen in the next section.

Once the entity has been retrieved, it can then be modified by calling the `modify` method. This method returns a new copy of the original entity with the same ID and version, but a modified body. The new entity is then used with `EntityStore::updateEntities` to commit the update.

```kotlin
val modifiedEntity = entity.modify { copy(price = price - 1.5) }

entityStore.updateEntities(modifiedEntity)
```

The trailing lambda passed to `existingEntity.modify` must return the new entity that will replace the existing one.

Dynadoc will always make sure no update has been made to the document between the time it was read and the time the update was committed. If a conflicting update has been made during that time, an exception of type `UpdateConflictException` will be thrown at the moment of committing the update.

## Deleting a document

To delete a document, simply set it to `null`.

```kotlin
val modifiedEntity = entity.modify { null }

entityStore.updateEntities(modifiedEntity)
```

## Advanced document queries

Advanced queries can be performed on the DynamoDB table.

The `query` or `scan` method of the `DynamoDbDocumentStore` class should be used.

```kotlin
val result = documentStore.scan {
    filterExpression("price BETWEEN :min AND :max")
    expressionAttributeValues(
        mapOf(
            ":min" to AttributeValue.fromN("100.00"),
            ":max" to AttributeValue.fromN("250.00")
        )
    )
}

val entities: Flow<JsonEntity<Product?>> = result.map(DefaultJsonSerializer::fromDocument)
```

## Atomic batch updates

There is often a need to atomically update multiple documents simultaneously. This can be achieved using the `BatchBuilder` class.

```kotlin
// Obtained via dependency injection
val entityStore: EntityStore
// Obtained externally (e.g. user input)
val invoiceId: DocumentKey
val productId: DocumentKey

entityStore.transaction {
    // Read the entities
    val invoice = entityStore.getEntity<Invoice>(invoiceId).ifExists()
        ?: error("Invoice ID $invoiceId not found.")
    val product = entityStore.getEntity<Product>(productId).ifExists()
        ?: error("Product ID $productId not found.")

    // Modify the entities
    modify(product) { copy(stockQuantity = stockQuantity - 1) }
    modify(invoice) { copy(total = total + product.entity.price) }
}
```

When the `transaction` scope completes, both documents will be updated together as part of an ACID transaction. If any of the documents have been modified between the time they were read and the time the scope completes, an exception of type `UpdateConflictException` will be thrown, and none of the changes will be committed to the database.

It is possible to automatically retry the transaction in case of conflict by passing a `RetryPolicy`.

```kotlin
val retryPolicy: RetryPolicy = retry(maxRetries = 3, pause = Duration.ofSeconds(5))

store.transaction(retryPolicy) {
    // Transaction code
}
```

## Using TiDB

`TiDbDocumentStore` implements the same `DocumentStore` interface, using the MySQL protocol of TiDB through R2DBC.

```kotlin
val connectionFactory: ConnectionFactory =
    ConnectionFactories.get("r2dbc:mysql://user:password@host:4000/database")

val documentStore = TiDbDocumentStore(connectionFactory, "documents")

// Create the table, clustered by hash of the partition key, partition key then sort key
documentStore.createTable()

val entityStore = EntityStore(documentStore, DefaultJsonSerializer)
```

The module only depends on the R2DBC SPI, so an R2DBC driver for MySQL, such as `io.asyncer:r2dbc-mysql`, must be added to the project. Every operation takes a connection from the `ConnectionFactory` and closes it when it completes, so a connection pool such as `r2dbc-pool` should be used. The driver can send the statements as text, which is the default with `r2dbc-mysql`, or as [prepared statements](#prepared-statements).

The table name can be qualified with a database, as in `"database.documents"`. TiDB 7.4 or later is required.

### Deleted documents

A deleted document keeps its row, so that its version is preserved, for a duration set by the `expiration` parameter of `TiDbDocumentStore`, which is 30 days by default:

```kotlin
val documentStore = TiDbDocumentStore(connectionFactory, "documents", expiration = Duration.ofDays(7))
```

The row is then removed by the TTL jobs of TiDB, which run every hour by default, so it can remain for some time after it has expired. This interval is an attribute of the table, and can be changed with `ALTER TABLE documents TTL_JOB_INTERVAL = '24h'`. Once the row is removed, the document is read with a version of 0, as if it had never existed.

### Concurrency

An update of a single document is a single `INSERT` or `UPDATE` statement, which only writes the document if it has the expected version.

An update of multiple documents (for example with `EntityStore.transaction`) is a transaction made of a few statements, which are sent one after the other on the same connection. Their number doesn't depend on the number of documents:

1. The transaction begins.
2. The versions of all the documents, including the checked ones, are read for update.
3. If a document doesn't have the expected version, nothing is written, the transaction is rolled back and an `UpdateConflictException` referring to that document is thrown.
4. The updated documents are written with a single statement. A checked document that doesn't exist is also written, then deleted by a second statement, so that a concurrent creation of the same document causes a conflict.
5. The transaction is committed.

An update of multiple documents therefore takes four round trips to the database in the most common case, where the documents exist and none of them is only checked.

Updates are optimistic transactions: they don't lock the documents while they run, and conflicts are detected by TiDB when the transaction is committed. If any of the documents of an update, including the checked ones, has been written by another transaction since the update started, the commit fails and an `UpdateConflictException` is thrown. TiDB doesn't indicate which document caused that conflict, so the exception then refers to the first document of the update. Under contention, using a `RetryPolicy` with `transaction` retries the update with fresh versions of the documents.

An update never waits for another update of the store. It can however wait if one of its documents is locked for a long time by a transaction of another application, such as a pessimistic transaction left open.

### Prepared statements

When TiDB receives a statement as text, it parses and plans it every time. Most of the statements of the store have the same text whatever their parameters (only the number of documents changes the text of the statements reading or updating multiple documents), so they can be prepared on the server: TiDB then reuses the plan it cached for the statement, which lowers its CPU usage. Since the SQL layer of TiDB is often the first resource to run out under a write-heavy load, this can increase the throughput of the store.

With `r2dbc-mysql`, the connections have to be created with `useServerPrepareStatement`:

```kotlin
val connectionFactory: ConnectionFactory = ConnectionPool(
    ConnectionPoolConfiguration.builder(
        MySqlConnectionFactory.from(
            MySqlConnectionConfiguration.builder()
                .host("host")
                .port(4000)
                .user("user")
                .password("password")
                .database("database")
                .useServerPrepareStatement()
                .sessionVariables("tidb_opt_fix_control='44830:ON'")
                .build()
        )
    ).build()
)
```

- A prepared statement and its cached plan belong to a connection, so the connections have to be reused, which a pool does. With a connection created for each operation, preparing statements only adds a round trip.
- TiDB doesn't cache the plans of the statements reading or writing several keys at once, which are used to read documents and to update multiple documents, unless `tidb_opt_fix_control` includes `44830:ON`. It is safe to enable with the store, which never sends the same key twice in a statement. Without it everything works, but these statements are planned every time.
- The statements updating a single document are cached without any setting.

### Queries

The documents of a partition can be retrieved, sorted by sort key, using the `scan` method. The range of sort keys is optional, with an inclusive start and an exclusive end:

```kotlin
val result = documentStore.scan("products", startSortKey = "A", endSortKey = "M")

val entities: Flow<JsonEntity<Product?>> = result.map(DefaultJsonSerializer::fromDocument)
```

Custom queries can be performed using the `query` method, which takes the condition of a `WHERE` clause and the values of its parameters. The condition can refer to the columns of the table, and use the [JSON functions](https://docs.pingcap.com/tidb/stable/json-functions) on the `body` column:

```kotlin
val result = documentStore.query(
    """
    partition_hash = CRC32(?) AND partition_key = ?
    AND body->'$.price' BETWEEN ? AND ?
    ORDER BY sort_key
    """,
    "products",
    "products",
    100,
    250,
)
```

To read a partition directly, the condition must specify both the hash of the partition key and the partition key, as above. A condition on `partition_key` alone, or on a range of partition keys, reads the whole table, since partitions are stored in the order of their hashes.

Both methods include deleted documents, which have a `NULL` body. They can be excluded from a query by adding `body IS NOT NULL` to the condition.

## License

Copyright 2023 Flavien Charlon

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
