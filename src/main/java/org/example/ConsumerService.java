package org.example;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.*;

@Slf4j
@Service
public class ConsumerService {

    Map<String, List<String>> messageCache = new HashMap<>();

    public Mono<List<String>> getNewMessages(String topic) {
        return Mono.just(popMessages(topic));
    }

    @KafkaListener(topicPattern = ".*", groupId = "1")
    private void listen(
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Payload String message) {
        log.info("Received message on topic '{}': {}", topic, message);
        cacheMessage(topic, message);
        log.info("Current cache state: {}", messageCache);
    }

    private List<String> popMessages(String topic) {
        List<String> res = Optional.ofNullable(messageCache.get(topic)).orElse(List.of());
        clearCache(topic);
        return res;
    }

    private void clearCache(String topic) {
        messageCache.put(topic, Arrays.asList());
    }

    private void cacheMessage(String topic, String message) {
        messageCache.compute(topic, (k, v) -> {
            List<String> res = new ArrayList<>(v != null ? v : List.of());
            res.add(message);
            return res;
        });
    }
}
