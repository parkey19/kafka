package com.example.kafka.service;

import com.example.kafka.core.KafkaManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.Collections;
import java.util.function.Function;
import java.util.logging.Level;

@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumerService {

    private final KafkaManager kafkaManager;

    String policyTopicName = "Policy";

    public void consume() {
        log.info("call consume");
        final ReceiverOptions<String, String> options = ReceiverOptions.<String, String>create(kafkaManager.getConsumerProps())
                .addAssignListener(partitions -> {
                    log.info("Partitions Assigned {}", partitions);
                })
                .addRevokeListener(receiverPartitions -> receiverPartitions.forEach(System.out::println))
                .subscription(Collections.singleton(policyTopicName));
        KafkaReceiver<String, String> receiver = KafkaReceiver.create(options);
        Flux.defer(receiver::receiveAutoAck)
                .flatMap(consumerRecordFlux -> consumerRecordFlux.publishOn(Schedulers.parallel()))
                .log()
                .map(this::commitAndConvertToInteger)
                .groupBy(Function.identity())
                .flatMap(this::sampling)
                .log(null, Level.INFO)
                .subscribe(record -> log.info("value:{}",record), Throwable::printStackTrace, () -> log.info("complete"));
    }

    private Integer commitAndConvertToInteger(ConsumerRecord<String, String> record) {
        return Integer.parseInt(record.value());
    }

    private Flux<Integer> sampling(reactor.core.publisher.GroupedFlux<Integer, Integer> groupedFlux) {
        return groupedFlux.sampleFirst(Duration.ofSeconds(5));
    }
}
