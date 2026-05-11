# Dynadoc

<a href="https://central.sonatype.com/artifact/org.pixode/dynadoc-core">![Maven Central Version](https://img.shields.io/maven-central/v/org.pixode/dynadoc-core)</a>

Dynadoc is a Kotlin library for using DynamoDB as a JSON document store. It manages the mapping between Kotlin objects and JSON documents.

## Concepts

### DynamoDB mapping

Dynadoc translates JSON documents to DynamoDB items by converting top-level keys into DynamoDB attributes.

It also adds a few special attributes that don't appear in the JSON, but appear in the `JsonEntity` objects:

- `partition_key`: The partition key of the DynamoDB table. 
- `sort_key`: The sort key of the DynamoDB table.
- `version`: An integer representing the version of the item, for optimistic concurrency management purposes.
- `deleted`: An attribute set on deleted objects. It contains a value that can be used with the TTL feature of DynamoDB to clear soft-deleted items from the table.

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

The following dependencies should be added to the project:

- [dynadoc](https://central.sonatype.com/artifact/org.pixode/dynadoc-core): The core library, responsible for storing and retrieving JSON documents in DynamoDB.
- [dynadoc-kotlinx-serialization](https://central.sonatype.com/artifact/org.pixode/dynadoc-kotlinx-serialization): The library in charge of serializing and deserializing JSON into Kotlin objects. This library relies internally on the `kotlinx-serialization-json` library to handle JSON serialization.

### Initialization

Dynadoc requires an instance of a `DynamoDbAsyncClient` object to construct the base `DynamoDbDocumentStore` object.

```kotlin
val client: DynamoDbAsyncClient = DynamoDbAsyncClient.builder()
    .credentialsProvider(DefaultCredentialsProvider.create())
    .build()

val documentStore: DynamoDbDocumentStore = DynamoDbDocumentStore(client, "tablename")
```

Then, an `EntityStore` object should be instantiated:

```kotlin
val entityStore: EntityStore = EntityStore(documentStore, DefaultJsonSerializer)
```

The `DefaultJsonSerializer` singleton relies on the default `Json` object, but it is possible to create an instance of the `KotlinJsonSerializer` and provide a custom `Json` object to customise the serializer settings. 

### Usage with dependency injection

When using a dependency injection framework such as Guice, a factory function such as this can be used:

```kotlin
@Provides
@Singleton
fun entityStore(awsCredentialsProvider: AwsCredentialsProvider): EntityStore {
    val client: DynamoDbAsyncClient = DynamoDbAsyncClient.builder()
        .credentialsProvider(awsCredentialsProvider)
        .build()

    val documentStore: DynamoDbDocumentStore = DynamoDbDocumentStore(client, "tablename")
    
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
val id: DocumentKey

val entity: JsonEntity<Product?> = entityStore.getEntity(key)
```

If the document does not exist, this method will return a "shadow" `JsonEntity<T>` object which has a null body and a version number of `0`. It is possible to update this "shadow" document the same way a normal document can be updated, which will result in the document being effectively created in the table.

It is possible to ensure the document exists by using the `ifExists` function.

```kotlin
val existingEntity = entity.ifExists() ?: error("The entity was not found.")
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

To delete a document, simply set it to null.

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

## License

Copyright 2023 Flavien Charlon

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
