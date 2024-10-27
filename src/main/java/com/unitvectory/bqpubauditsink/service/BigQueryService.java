/*
 * Copyright 2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.unitvectory.bqpubauditsink.service;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.threeten.bp.Duration;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;

import javax.annotation.PreDestroy;

/**
 * The BigQuery Service
 * 
 * @author Jared Hatfield (UnitVectorY Labs)
 */
@Service
public class BigQueryService {

    private final BigQueryWriteClient client;
    private final JsonStreamWriter streamWriter;
    private final TableName parentTable;

    private final Phaser inflightRequestCount = new Phaser(1);

    public BigQueryService(@Value("${project.id}") String projectId,
            @Value("${dataset.name}") String datasetName,
            @Value("${table.name}") String tableName) throws DescriptorValidationException, IOException {
        this.client = BigQueryWriteClient.create();
        this.parentTable = TableName.of(projectId, datasetName, tableName);
        this.streamWriter = createStreamWriter(parentTable.toString());
    }

    private JsonStreamWriter createStreamWriter(String tableName) throws DescriptorValidationException, IOException {
        RetrySettings retrySettings = RetrySettings.newBuilder()
                .setInitialRetryDelay(Duration.ofMillis(500))
                .setRetryDelayMultiplier(1.1)
                .setMaxAttempts(5)
                .setMaxRetryDelay(Duration.ofMinutes(1))
                .build();

        // Using the default stream for the specified table
        try {
            return JsonStreamWriter.newBuilder(tableName, client)
                    .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(100)))
                    .setRetrySettings(retrySettings)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void insert(JSONObject jsonObject) throws IOException, DescriptorValidationException  {
        JSONArray jsonArray = new JSONArray();
        jsonArray.put(jsonObject);
        
        // Append asynchronously for increased throughput.
        ApiFuture<AppendRowsResponse> future = streamWriter.append(jsonArray);
        ApiFutures.addCallback(future, new AppendCompleteCallback(), MoreExecutors.directExecutor());
        inflightRequestCount.register();
    }

    @PreDestroy
    public void cleanup() throws IOException {
        inflightRequestCount.arriveAndAwaitAdvance();
        streamWriter.close();
        client.close();
    }

    private class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {
        public void onSuccess(AppendRowsResponse response) {
            System.out.format("Append success\n");
            inflightRequestCount.arriveAndDeregister();
        }

        public void onFailure(Throwable throwable) {
            // Handle the error appropriately (log it, retry, etc.)
            System.err.println("Append failed: " + throwable.getMessage());
            inflightRequestCount.arriveAndDeregister();
        }
    }
}
