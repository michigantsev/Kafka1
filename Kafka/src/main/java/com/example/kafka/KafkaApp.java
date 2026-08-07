package com.example.kafka;

import com.example.kafka.service.producer.ProducerService;
import com.example.kafka.topology.MessageFilterTopology;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.Arrays;
import java.util.Properties;


@SpringBootApplication
@EnableAsync
public class KafkaApp implements CommandLineRunner {

    private final ProducerService producerService;
    private static final String BOOTSTRAP_SERVERS = "kafka-0:9094";

    public KafkaApp(ProducerService producerService) {
        this.producerService = producerService;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaApp.class, args);
    }

    @Override
    public void run(String... args) {
        createTopics();

        KafkaStreams streams = startKafkaStreams();

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        producerService.runProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static void createTopics() {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic messagesTopic = new NewTopic("messages", 1, (short) 1);
            NewTopic filteredMessagesTopic = new NewTopic("filtered_messages", 1, (short) 1);
            NewTopic blockedUsersTopic = new NewTopic("blocked_users", 1, (short) 1);

            adminClient.createTopics(Arrays.asList(messagesTopic, filteredMessagesTopic, blockedUsersTopic)).all().get();
            System.out.println("[AdminClient] Топики успешно созданы на " + BOOTSTRAP_SERVERS);
        } catch (Exception e) {
            System.err.println("[AdminClient] Ошибка топиков: " + e.getMessage());
        }
    }

    private KafkaStreams startKafkaStreams() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "chat-filter-service");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        MessageFilterTopology.buildTopology(streamsBuilder);
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        return streams;
    }
}


