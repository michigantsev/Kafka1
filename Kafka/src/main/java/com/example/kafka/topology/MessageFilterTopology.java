package com.example.kafka.topology;

import com.example.kafka.model.Message;
import com.example.kafka.model.UserBlocking;
import com.example.kafka.processor.CensorProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;


public class MessageFilterTopology {
    public static final String BLOCKED_STORE = "blocked-users-store";
    public static final String WORDS_STORE = "forbidden-words-store";

    public static void buildTopology(StreamsBuilder builder) {
       JsonSerde<Message> messageSerde = new JsonSerde<>(Message.class);
       JsonSerde<UserBlocking> blockSerde = new JsonSerde<>(UserBlocking.class);

        GlobalKTable<String, UserBlocking> blockedUsersGlobalTable = builder.globalTable(
                "blocked_users",
                Consumed.with(Serdes.String(), blockSerde),
                Materialized.<String, UserBlocking, KeyValueStore<Bytes, byte[]>>as(BLOCKED_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(blockSerde)
        );

        builder.globalTable(
                "forbidden_words",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(WORDS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );

        builder.stream("messages", Consumed.with(Serdes.String(), messageSerde))
                .selectKey((key, message) -> message.recipientId)
                .leftJoin(blockedUsersGlobalTable,
                        (key, message) -> message.recipientId,
                        (message, userBlocking) -> {
                            if (userBlocking != null && userBlocking.blockedUserId != null
                                    && userBlocking.blockedUserId.contains(message.userId)) {
                                System.out.println("[MessageFilterTopology] Метка блокировки для пользователя: " + message.userId);
                                Message blockedMarker = new Message();
                                blockedMarker.userId = "BLOCKED";
                                return blockedMarker;
                            }
                            return message;
                        }
                )
                .filter((key, message) -> message != null && !"BLOCKED".equals(message.userId))
                .selectKey((key, message) -> message.userId)
                .process(() -> new CensorProcessor(WORDS_STORE))
                .to("filtered_messages", Produced.with(Serdes.String(), messageSerde));
   }
}


