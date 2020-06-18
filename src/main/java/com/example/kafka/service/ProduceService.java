package com.example.kafka.service;

import com.example.kafka.core.KafkaManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Collections;
import java.util.function.Function;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProduceService {

    private final KafkaManager kafkaManager;

    public void produce() {
        log.info("call produce");
        final Flux<SenderRecord<String, String, String>> records = Flux.range(1,30000)
                .doOnNext(i -> log.info("Create - {}", i))
                .map(Object::toString)
                .map(i -> SenderRecord.create(new ProducerRecord<>("Policy", i, i), i));

        kafkaManager.producer(records)
                .subscribe();
    }

    public void produce2() {
        final Flux<SenderRecord<String, String, String>> records = Flux.range(29900,100)
                .doOnNext(i -> log.info("Create - {}", i))
                .map(Object::toString)
                .map(i -> SenderRecord.create(new ProducerRecord<>("Policy", i, i), i));

        kafkaManager.producer(records)
                .subscribe();
    }
}
