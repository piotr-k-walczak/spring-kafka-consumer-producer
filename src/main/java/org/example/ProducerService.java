package org.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

@Service
public class ProducerService {

    @Autowired
    KafkaTemplate<String, String> kafkaStringTemplate;

    public Mono<SendResult<String, String>> pushMessage(String topic, String key, String value) {
        CompletableFuture<SendResult<String, String>> res = kafkaStringTemplate.send(topic, key, value);
        return Mono.fromFuture(res);
    }
}
