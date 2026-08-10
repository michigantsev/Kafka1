package com.example.kafka;

import com.example.kafka.model.Message;
import com.example.kafka.service.producer.ProducerService;
import com.example.kafka.topology.MessageFilterTopology;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.Arrays;
import java.util.Properties;


@SpringBootApplication
@EnableAsync
@EnableKafkaStreams
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

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        producerService.runProducer();
    }

    private static void createTopics() {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic messagesTopic = new NewTopic("messages", 1, (short) 1);
            NewTopic filteredMessagesTopic = new NewTopic("filtered_messages", 1, (short) 1);
            NewTopic blockedUsersTopic = new NewTopic("blocked_users", 1, (short) 1);
            NewTopic forbiddenWordsTopic = new NewTopic("forbidden_words", 1, (short) 1);

            adminClient.createTopics(Arrays.asList(messagesTopic, filteredMessagesTopic, blockedUsersTopic, forbiddenWordsTopic)).all().get();
            System.out.println("[AdminClient] Топики успешно созданы на " + BOOTSTRAP_SERVERS);
        } catch (Exception e) {
            System.err.println("[AdminClient] Ошибка топиков: " + e.getMessage());
        }
    }

    @Bean
    public KStream<String, Message> kStream(StreamsBuilder streamsBuilder) {
        MessageFilterTopology.buildTopology(streamsBuilder);

        JsonSerde<Message> messageSerde = new JsonSerde<>(Message.class);
        return streamsBuilder.stream("messages", org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), messageSerde));
    }
}


