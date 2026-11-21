package com.duoc.ep5;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class Ep5KafkaStreamsApplication {
    public static void main(String[] args) {
        SpringApplication.run(Ep5KafkaStreamsApplication.class, args);
    }
}
