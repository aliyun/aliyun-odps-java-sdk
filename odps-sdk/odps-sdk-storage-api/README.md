# MaxCompute Storage API

API Doc: https://aliyuque.antfin.com/odps/tunnel/gwvyc62qy5bnwngg

The MaxCompute Storage API provides high-performance data reading and writing capabilities for MaxCompute tables using the Apache Arrow format.

## Overview

The Storage API enables efficient data transfer between MaxCompute tables and client applications by leveraging the Apache Arrow columnar format. It provides both read and write sessions with support for parallel processing.

## Key Features

1. **High Performance**: Uses Apache Arrow for efficient data processing and transfer
2. **Parallel Processing**: Supports reading and writing data in parallel streams
3. **Transactional**: Write operations are atomic and transactional
4. **Blob Support**: Handles large binary objects separately from structured data
5. **Easy to Use**: Simple builder pattern for creating sessions

## Getting Started

### Creating a Storage Client

```java
// Get the storage client from the Odps instance
StorageClient storageClient = odps.storage();

// The client is thread-safe and designed to be long-lived
```

### Reading Data

```java
// Create a read session builder
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
TableReadSessionBuilder readBuilder = storageClient.createReadSessionBuilder(tableId);

// Build the session
TableReadSession readSession = readBuilder.build();

// Get the schema
Schema schema = readSession.getArrowSchema();

// Get the streams for parallel reading
List<ReadStream> streams = readSession.getStreams();

// Process each stream in parallel
for (ReadStream stream : streams) {
    try (TableArrowReader reader = readSession.newArrowReader(stream)) {
        while (reader.read()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            // Process the data in root
        }
    }
}
```

### Writing Data

```java
// Create a write session builder
TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
TableWriteSessionBuilder writeBuilder = storageClient.createWriteSessionBuilder(tableId);

// Build the session
TableWriteSession writeSession = writeBuilder.build();

try {
    // Create a writer for this session
    try (TableArrowWriter writer = writeSession.newArrowWriter()) {
        // Write data in batches
        VectorSchemaRoot batch = // ... prepare your data batch
        writer.write(batch);
    }
    
    // Commit the transaction to make data visible
    writeSession.commit();
} catch (Exception e) {
    // If an exception occurs, the session will be automatically aborted
    throw e;
}
```

### Reading Blobs

```java
// Read blob data using a reference
String blobReference = // ... obtained from table data
try (InputStream blobData = storageClient.readBlob(blobReference)) {
    // Process the blob data
}
```

## API Components

### Core Interfaces

- `StorageClient`: Main entry point for the Storage API
- `TableReadSessionBuilder`: Builder for creating read sessions
- `TableWriteSessionBuilder`: Builder for creating write sessions

### Read Session Components

- `TableReadSession`: Represents a read session for a table
- `ReadStream`: Represents a single stream within a read session
- `TableArrowReader`: Reader for consuming data from a stream

### Write Session Components

- `TableWriteSession`: Represents a write session for a table
- `TableArrowWriter`: Writer for sending data to a stream

## Thread Safety

- `StorageClient` is thread-safe and designed for long-lived usage
- Session builders are lightweight and can be created per operation
- Individual sessions and their components are not thread-safe

## Resource Management

Always use try-with-resources statements when working with readers, writers, and sessions to ensure proper resource cleanup.

## Future Enhancements

- Support for partition filtering in read sessions
- Support for column projection in read sessions
- Enhanced statistics and monitoring capabilities
- Support for additional data formats