/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.storage;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.storage.internal.BlobManagerImpl;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.StubSettings;
import com.aliyun.odps.storage.read.ArrowReaderImpl;
import com.aliyun.odps.storage.read.InstanceReadSessionBuilder;
import com.aliyun.odps.storage.read.TableReadSessionBuilder;
import com.aliyun.odps.storage.settings.HttpSettings;
import com.aliyun.odps.storage.write.TableWriteSessionBuilder;
import com.aliyun.odps.table.InstanceIdentifier;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.arrow.ArrowReader;

/**
 * Implementation of the MaxCompute Storage API client.
 *
 * <p>This client provides high-performance read and write operations for MaxCompute tables
 * using the Storage API. It supports both table and volume operations with features like
 * compression, retry handling, and connection management.
 *
 * <p>Example usage for reading from a table:
 * <pre>{@code
 * MaxStorageClient client = MaxStorageClient.builder()
 *     .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
 *     .credentialsProvider(credentialsProvider)
 *     .build();
 *
 * TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
 * try (TableReadSession session = client.createReadSessionBuilder(tableId).build()) {
 *     List<InputSplit> splits = session.getSplits();
 *     for (InputSplit split : splits) {
 *         try (ArrowReader reader = session.createArrowReaderBuilder(split).build()) {
 *             // Process data from reader
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p>Example usage for writing to a table:
 * <pre>{@code
 * MaxStorageClient client = MaxStorageClient.builder()
 *     .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
 *     .credentialsProvider(credentialsProvider)
 *     .build();
 *
 * TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
 * try (TableWriteSession session = client.createWriteSessionBuilder(tableId).build()) {
 *     TableArrowWriter writer = session.newArrowWriterBuilder().build();
 *     // Write data using writer
 *     writer.close();
 *     session.commit();
 * }
 * }</pre>
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class MaxStorageClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(MaxStorageClient.class);

  private BufferAllocator allocator;

  private StorageStub storageStub;


  /**
   * Constructs a new MaxStorageClient with the provided builder configuration.
   *
   * @param builder The builder containing client configuration parameters
   */
  public MaxStorageClient(Builder builder) {
    StubSettings stubSettings = StubSettings.newBuilder()
      .withEndpoint(builder.endpoint)
      .withTunnelEndpoint(builder.tunnelEndpoint)
      .withCredentialsProvider(builder.credentialsProvider)
      .withQuota(builder.quota)
      .withRegion(builder.region)
      .withProject(builder.project)
      .withHttpSettings(builder.httpSettings)
      .withUserAgent(builder.userAgent)
      .build();
    this.storageStub = new StorageStub(stubSettings);
    this.allocator = builder.bufferAllocator;
    if (allocator == null) {
      this.allocator = new RootAllocator();
    }
  }

  /**
   * Creates a new builder for constructing MaxStorageClient instances.
   *
   * <p>This is the recommended way to create MaxStorageClient instances, allowing
   * configuration of various client settings such as endpoint, credentials,
   * retry policies, and HTTP settings.
   *
   * @return A new Builder instance for configuring and creating MaxStorageClient instances
   */
  public static Builder builder() {
    return new Builder();
  }

  public BlobManager openBlobManager() {
    return new BlobManagerImpl(storageStub);
  }

  /**
   * Previews data from a table with the specified filters and limit.
   *
   * <p>This method returns an ArrowReader that must be closed by the caller to release resources.
   * It is recommended to use try-with-resources to ensure the reader is properly closed.
   *
   * <p>Example usage:
   * <pre>{@code
   * TableIdentifier tableId = TableIdentifier.of("my_project", "my_table");
   * PartitionSpec partition = new PartitionSpec("pt='20250101'");
   * List<String> columns = Arrays.asList("col1", "col2");
   * try (ArrowReader reader = client.previewTable(tableId, partition, columns, 100)) {
   *     while (reader.loadNextBatch()) {
   *         VectorSchemaRoot root = reader.getVectorSchemaRoot();
   *         // Process data
   *     }
   * }
   * }</pre>
   *
   * @param table The identifier of the table to preview
   * @param partition The partition specification to filter data (can be null for all partitions)
   * @param columns The list of column names to read (can be null for all columns)
   * @param limit The maximum number of rows to read (can be null for no limit)
   * @return An ArrowReader that must be closed by the caller
   * @throws MaxStorageException if the table does not exist or access is denied
   * @throws ClientException if there is a client-side error
   */
  public ArrowReader previewTable(TableIdentifier table, PartitionSpec partition,
                                  List<String> columns, Integer limit) {
    String partitionParam = partition == null ? null : partition.toString(false, false);
    return new ArrowReaderImpl(storageStub.preview(table, partitionParam, columns, limit),
                               this.allocator);
  }

  /**
   * Creates a new builder for a table write session.
   *
   * <p>This method initializes a write session builder that can be configured with
   * various options before creating the actual write session. The write session
   * allows writing data to a MaxCompute table using the Arrow format.
   *
   * @param table The identifier of the target table for writing data
   * @return A new TableWriteSessionBuilder instance to configure the write session
   */
  public TableWriteSessionBuilder createTableWriteSessionBuilder(TableIdentifier table) {
    return new TableWriteSessionBuilder(storageStub, allocator, table);
  }

  /**
   * Creates a new builder for a table read session.
   *
   * <p>This method initializes a read session builder that can be configured with
   * various options before creating the actual read session. The read session
   * allows reading data from a MaxCompute table using the Arrow format.
   *
   * @param table The identifier of the source table for reading data
   * @return A new TableReadSessionBuilder instance to configure the read session
   */
  public TableReadSessionBuilder createTableReadSessionBuilder(TableIdentifier table) {
    return new TableReadSessionBuilder(storageStub, allocator, table);
  }


  /**
   * Creates a new builder for an instance read session.
   *
   * <p>This method initializes a read session builder that can be configured with
   * various options before creating the actual read session. The read session
   * allows reading data from a MaxCompute instance using the Arrow format.
   *
   * @param instance The identifier of the source instance for reading data
   * @return A new InstanceReadSessionBuilder instance to configure the read session
   */
  public InstanceReadSessionBuilder createInstanceReadSessionBuilder(InstanceIdentifier instance) {
    return new InstanceReadSessionBuilder(storageStub, allocator, instance);
  }


  /**
   * Closes the client and releases all associated resources.
   *
   * <p>This method closes the underlying buffer allocator and storage stub, releasing
   * any resources such as network connections, thread pools, and memory buffers.
   * It should be called when the client is no longer needed to prevent resource leaks.
   */
  public void close() {
    this.allocator.close();
    this.storageStub.close();
  }

  /**
   * Builder class for constructing MaxStorageClient instances.
   *
   * <p>This builder allows configuration of various client settings including:
   * <ul>
   *   <li>Endpoint and tunnel endpoint URLs</li>
   *   <li>Project and region information</li>
   *   <li>Credentials provider for authentication</li>
   *   <li>Retry settings for handling transient failures</li>
   *   <li>HTTP settings for connection management</li>
   *   <li>Buffer allocator for memory management</li>
   * </ul>
   *
   * <p>Example usage:
 * <pre>{@code
 * MaxStorageClient client = MaxStorageClient.builder()
 *     .endpoint("https://service.cn-hangzhou.maxcompute.aliyun.com/api")
 *     .credentialsProvider(credentialsProvider)
 *     .httpSettings(HttpSettings.newBuilder().build())
 *     .build();
 * }</pre>
   */
  public static class Builder {

    private String project;

    private String endpoint;

    private String quota;

    private String region;

    private String tunnelEndpoint;

    private ICredentialsProvider credentialsProvider;

    private HttpSettings httpSettings = HttpSettings.newBuilder().build();

    private String userAgent;

    private BufferAllocator bufferAllocator;

    /**
     * Sets the MaxCompute service endpoint.
     *
     * @param endpoint The MaxCompute service endpoint URL
     * @return This builder instance for method chaining
     */
    public Builder endpoint(String endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    /**
     * Sets the MaxCompute project name.
     *
     * @param project The project name
     * @return This builder instance for method chaining
     */
    public Builder project(String project) {
      this.project = project;
      return this;
    }

    /**
     * Sets the MaxCompute tunnel endpoint.
     *
     * @param tunnelEndpoint The tunnel endpoint URL
     * @return This builder instance for method chaining
     */
    public Builder tunnelEndpoint(String tunnelEndpoint) {
      this.tunnelEndpoint = tunnelEndpoint;
      return this;
    }

    /**
     * Sets the MaxCompute region.
     *
     * @param region The region name
     * @return This builder instance for method chaining
     */
    public Builder region(String region) {
      this.region = region;
      return this;
    }

    /**
     * Sets the MaxCompute quota.
     *
     * @param quota The quota name
     * @return This builder instance for method chaining
     */
    public Builder quota(String quota) {
      this.quota = quota;
      return this;
    }

    /**
     * Sets the credentials provider for authentication.
     *
     * @param credentialsProvider The credentials provider
     * @return This builder instance for method chaining
     */
    public Builder credentialsProvider(ICredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    /**
     * Sets the HTTP settings for connection management.
     *
     * @param httpSettings The HTTP settings configuration
     * @return This builder instance for method chaining
     */
    public Builder httpSettings(HttpSettings httpSettings) {
      this.httpSettings = httpSettings;
      return this;
    }

    /**
     * Sets the buffer allocator for memory management.
     *
     * @param bufferAllocator The buffer allocator
     * @return This builder instance for method chaining
     */
    public Builder bufferAllocator(BufferAllocator bufferAllocator) {
      this.bufferAllocator = bufferAllocator;
      return this;
    }

    /**
     * Appends a custom string to the 'User-Agent' header for all MaxStorage requests.
     * Identifying the data source helps in traffic analysis and debugging.
     *
     * @param userAgent The custom user agent string. It will be placed at the end of the
     *                  standard SDK identifier:
     *                  {@code JavaSDK/%s (id:%s; revision:%s; java-version:%s; region:%s) [userAgent]}
     * @return This builder instance
     */
    public Builder userAgent(String userAgent) {
      this.userAgent = userAgent;
      return this;
    }


    /**
     * Builds and returns a new MaxStorageClient instance with the configured settings.
     *
     * <p>If no tunnel endpoint is explicitly set, this method will attempt to retrieve
     * the tunnel endpoint from the MaxCompute service using the configured endpoint,
     * project, and credentials.
     *
     * @return A new MaxStorageClient instance
     */
    public MaxStorageClient build() {
      return new MaxStorageClient(this);
    }
  }
}
