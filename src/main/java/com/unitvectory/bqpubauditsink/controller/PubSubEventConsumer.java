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
package com.unitvectory.bqpubauditsink.controller;

import java.util.Base64;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
 
import com.google.protobuf.InvalidProtocolBufferException;
import com.unitvectory.bqpubauditsink.model.PubSubPublish;
import com.unitvectory.bqpubauditsink.service.BigQueryService;

import lombok.extern.slf4j.Slf4j;

/**
 * The Pub/Sub event controller
 * 
 * @author Jared Hatfield (UnitVectorY Labs)
 */
@RestController
@Slf4j
public class PubSubEventConsumer {

    @Autowired
    private BigQueryService bigQueryService;

    @PostMapping(value = "/pubsub", consumes = "application/json")
    public void handleFirestoreEvent(@RequestBody PubSubPublish data) throws InvalidProtocolBufferException {
        // Decode the Base64 encoded message data
        String jsonString = new String(Base64.getDecoder().decode(data.getMessage().getData()));

        // Parse the decoded string into a JSONObject
        JSONObject jsonObject = new JSONObject(jsonString);

        // Extract required fields
        String timestamp = jsonObject.optString("timestamp", null);
        String database = jsonObject.optString("database", null);
        String documentPath = jsonObject.optString("documentPath", null);

        if (timestamp == null || database == null || documentPath == null) {
            log.warn("Missing required fields, skipping record.");
            return;
        }

        JSONObject value = jsonObject.optJSONObject("value");
        JSONObject oldValue = jsonObject.optJSONObject("oldValue");

        // Prepare the JSON object for BigQuery
        JSONObject record = new JSONObject();
        record.put("timestamp", timestamp);
        record.put("database", database);
        record.put("documentPath", documentPath);
        record.put("value", value != null ? value.toString() : null);
        record.put("oldValue", oldValue != null ? oldValue.toString() : null);
        record.put("tombstone", value == null);

        // Insert the record into BigQuery
        try {
            this.bigQueryService.insert(record);
        } catch (Exception e) {
            log.error("Failed to insert record into BigQuery", e);
            return;
        } 
    }
}