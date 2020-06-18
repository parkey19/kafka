package com.example.kafka.Controller;

import com.example.kafka.service.ConsumerService;
import com.example.kafka.service.ProduceService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class KafkaController {

    private final ProduceService produceService;

    private final ConsumerService consumerService;

    @GetMapping("/start")
    public String start() {
//        consumerService.consume();
        produceService.produce();
        produceService.produce();
        return "start";
    }

    @GetMapping("/dis")
    public String dis() {
//        consumerService.consume();
        produceService.produce2();
        return "dis";
    }

    @GetMapping("/con")
    public String con() {
        consumerService.consume();
        return "con";
    }
}
