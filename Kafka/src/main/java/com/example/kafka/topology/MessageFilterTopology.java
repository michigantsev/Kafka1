package com.example.kafka.topology;

import com.example.kafka.model.Message;
import com.example.kafka.model.UserBlocking;
import com.example.kafka.processor.BlockProcessor;
import com.example.kafka.processor.CensorProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.DslStoreSuppliers;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.apache.kafka.streams.processor.api.*;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class MessageFilterTopology {
    public static final String BLOCKED_STORE = "blocked-users-store";
    // в задании написано, что список должен обновляться динамически, но топика для него в списке нет -- что нужно сделать не понятно
    public static final Set<String> FORBIDDEN_WORDS = ConcurrentHashMap.newKeySet();
    static {
        FORBIDDEN_WORDS.add("яндекс");
    }


    public static void buildTopology(StreamsBuilder builder) {

       JsonSerde<Message> messageSerde = new JsonSerde<>(Message.class);
       JsonSerde<UserBlocking> blockSerde = new JsonSerde<>(UserBlocking.class);

        builder.globalTable(
                "blocked_users",
                Consumed.with(Serdes.String(), blockSerde),
                Materialized.<String, UserBlocking, KeyValueStore<Bytes, byte[]>>as(BLOCKED_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(blockSerde)
        );

        builder.stream("messages", Consumed.with(Serdes.String(), messageSerde))
                .process(() -> new BlockProcessor(BLOCKED_STORE))
                .process(() -> new CensorProcessor())
                .to("filtered_messages", Produced.with(Serdes.String(), messageSerde));
   }

}


