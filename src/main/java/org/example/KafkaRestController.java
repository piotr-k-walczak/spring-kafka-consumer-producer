package org.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
public class KafkaRestController {

    @Autowired
    private ProducerService producerService;

    @Autowired
    private ConsumerService consumerService;

    @GetMapping("/{topic}")
    Mono<List<String>> getNewMessages(@PathVariable String topic) {
        return consumerService.getNewMessages(topic);
    }

    @PostMapping("/{topic}")
    Mono<String> postMessage(@PathVariable String topic, @RequestBody PushMessage message) {
        return producerService.pushMessage(topic, message.key(), message.value()).map(SendResult::toString);
    }
}
