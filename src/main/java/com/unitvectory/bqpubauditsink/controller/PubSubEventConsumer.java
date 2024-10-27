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
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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

        // Parse the decoded string into a JsonObject
        JsonObject jsonObject = JsonParser.parseString(jsonString).getAsJsonObject();

        // Extract fields from the JsonObject
        String timestamp = jsonObject.has("timestamp") ? jsonObject.get("timestamp").getAsString() : null;
        String database = jsonObject.has("database") ? jsonObject.get("database").getAsString() : null;
        String documentPath = jsonObject.has("documentPath") ? jsonObject.get("documentPath").getAsString() : null;

        // These fields are required, if one is missing, skip the record
        if (timestamp == null) {
            log.warn("Timestamp is missing, skipping record.");
            return;
        } else if (database == null) {
            log.warn("Database is missing, skipping record.");
            return;
        } else if (documentPath == null) {
            log.warn("Document path is missing, skipping record.");
            return;
        }

        JsonObject value = jsonObject.has("value") ? jsonObject.getAsJsonObject("value") : null;
        JsonObject oldValue = jsonObject.has("oldValue") ? jsonObject.getAsJsonObject("oldValue") : null;

        // Create a Map to hold the row content
        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("timestamp", timestamp);
        rowContent.put("database", database);
        rowContent.put("documentPath", documentPath);
        rowContent.put("value", value != null ? value.toString() : null);
        rowContent.put("oldValue", oldValue != null ? oldValue.toString() : null);
        rowContent.put("tombstone", value == null);

        // Insert the record
        this.bigQueryService.insert(rowContent);
    }
}