# Dynadoc

<a href="https://central.sonatype.com/artifact/org.pixode/dynadoc">![Maven Central Version](https://img.shields.io/maven-central/v/org.pixode/dynadoc)</a>

Dynadoc is a Kotlin library for using DynamoDB, MongoDB or TiKV as a JSON document store. It manages the mapping between Kotlin objects and JSON documents.

## Concepts

### DynamoDB mapping

Dynadoc translates JSON documents to DynamoDB items by converting top-level keys into DynamoDB attributes.

It also adds a few special attributes that don't appear in the JSON, but appear in the `JsonEntity` objects:

- `partition_key`: The partition key of the DynamoDB table. 
- `sort_key`: The sort key of the DynamoDB table, holding the local key of the document.
- `version`: An integer representing the version of the item, for optimistic concurrency management purposes.
- `deleted`: An attribute set on deleted objects. It contains a value that can be used with the TTL feature of DynamoDB to clear soft-deleted items from the table.

### MongoDB mapping

With MongoDB, top-level keys of the JSON document become fields of the MongoDB document, and the following reserved fields are added:

- `_id`: An embedded document of the form `{ pk, lk }`, where `pk` holds the partition key and `lk` holds the local key.
- `_version`: An integer representing the version of the document, for optimistic concurrency management purposes.
- `_deleted`: A date set on deleted documents, used by a TTL index to clear soft-deleted documents from the collection.

These field names cannot be used in document bodies.

### TiKV mapping

With TiKV, each document is stored as a single key-value pair:

- The key is the concatenation of the first 8 bytes of the SHA-256 hash of a configurable namespace, the first 16 bytes of the SHA-256 hash of the partition key, and the local key, strings being encoded in UTF-8. Partitions are spread evenly across the key space of the namespace, while the documents of a partition are stored together and sorted by local key.
- The value is a UTF-8 encoded JSON object of the form `{ partition_key, local_key, version, body }`, where `partition_key` and `local_key` hold the partition key and local key, `version` the version of the document and `body` the JSON document. Since the body is nested, there are no reserved field names.

Deleted documents are kept with a `null` body so that their version is preserved. TiKV transactions don't support TTLs, so they are never removed automatically.

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
    val localKey: String,
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

To use MongoDB instead, add a dependency to [dynadoc-mongodb](https://central.sonatype.com/artifact/org.pixode/dynadoc-mongodb), which references the MongoDB Kotlin coroutine driver. `MongoDbDocumentStore` is in the `org.pixode.dynadoc.mongodb` package. See [Using MongoDB](#using-mongodb).

To use TiKV, add a dependency to `dynadoc-tikv`, which references the TiKV Java client. `TiKVDocumentStore` is in the `org.pixode.dynadoc.tikv` package. See [Using TiKV](#using-tikv).

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
    localKey = "product",
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

## Using MongoDB

`MongoDbDocumentStore` implements the same `DocumentStore` interface as `DynamoDbDocumentStore`, so it can be used with `EntityStore` in the same way.

```kotlin
val client: MongoClient = MongoClient.create("mongodb://localhost:27017/?replicaSet=rs0")

val documentStore = MongoDbDocumentStore(client, "database", "collection")

// Create the collection, clustered by document key, and the TTL index for deleted documents
documentStore.createCollection()

val entityStore = EntityStore(documentStore, DefaultJsonSerializer)
```

Updating a single document works with any deployment. Updating multiple documents atomically (for example with `EntityStore.transaction`) relies on MongoDB transactions, which require a replica set or a sharded cluster.

`createCollection` creates a [clustered collection](https://www.mongodb.com/docs/manual/core/clustered-collections/), which stores documents ordered by `_id`, so that the documents of a partition are stored together and sorted by local key. An existing collection cannot be converted to a clustered collection.

On a sharded cluster, the collection should be sharded using the partition key as the shard key:

```javascript
sh.shardCollection("database.collection", { "_id.pk": 1 })
```

Read concern and read preference are configured on the `MongoClient`.

Custom queries can be performed using the `find` method:

```kotlin
val result = documentStore.find(
    Filters.and(
        Filters.gte("_id", BsonDocument("pk", BsonString("products")).append("lk", BsonString("A"))),
        Filters.lt("_id", BsonDocument("pk", BsonString("products")).append("lk", BsonString("M"))),
        Filters.eq("_id.pk", "products"),
        Filters.gte("price", 100),
        Filters.lte("price", 250),
    ),
) {
    sort(Sorts.ascending("_id"))
}

val entities: Flow<JsonEntity<Product?>> = result.map(DefaultJsonSerializer::fromDocument)
```

To query a range of local keys, the range should be expressed on the whole `_id`, with `pk` before `lk`, rather than on `_id.lk`. This lets MongoDB scan only that range of the clustered collection, and return the documents already sorted by `_id`. A range on `_id.lk` reads every document of the partition instead.

The separate condition on `_id.pk` lets a sharded cluster send the query to a single shard. Without it, a range on `_id` is sent to every shard.

## Using TiKV

`TiKVDocumentStore` implements the same `DocumentStore` interface, using the transactional API of TiKV.

```kotlin
val session: TiSession = TiSession.create(TiConfiguration.createDefault("pd-host:2379"))

val documentStore = TiKVDocumentStore(session, "products")

val entityStore = EntityStore(documentStore, DefaultJsonSerializer)
```

The namespace isolates the documents of a store from the other data of the cluster, in the same way as a table or a collection. Its hash is used as the key prefix, so all namespaces have the same key length.

Updates are optimistic transactions: the documents are read at the start timestamp of the transaction to check their versions, then written using the two-phase commit protocol. Checked documents are locked without being modified, so that a concurrent write to any of the documents causes an `UpdateConflictException`.

The documents of a partition can be retrieved, sorted by local key, using the `scan` method. The range of local keys is optional, with an inclusive start and an exclusive end:

```kotlin
val result = documentStore.scan("products", startLocalKey = "A", endLocalKey = "M")

val entities: Flow<JsonEntity<Product?>> = result.map(DefaultJsonSerializer::fromDocument)
```

Reads use a snapshot at the latest timestamp, so they are strongly consistent.

## License

Copyright 2023 Flavien Charlon

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
