package com.example.kafka;

import com.example.kafka.service.consumer.BatchMessageConsumerService;
import com.example.kafka.service.consumer.SingleMessageConsumerService;
import com.example.kafka.service.producer.ProducerService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class KafkaApp implements CommandLineRunner {

    private final ProducerService producerService;
    private final SingleMessageConsumerService singleConsumer;
    private final BatchMessageConsumerService batchConsumer;

    public KafkaApp(ProducerService producerService,
                    SingleMessageConsumerService singleConsumer,
                    BatchMessageConsumerService batchConsumer) {
        this.producerService = producerService;
        this.singleConsumer = singleConsumer;
        this.batchConsumer = batchConsumer;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaApp.class, args);
    }

    @Override
    public void run(String... args) {
        singleConsumer.runConsumer();
        batchConsumer.runConsumer();

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        producerService.runProducer();
    }
}


