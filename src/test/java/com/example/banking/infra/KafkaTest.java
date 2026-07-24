package com.example.banking.infra;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves Kafka connectivity: a message published via the auto-configured {@link KafkaTemplate}
 * round-trips through the Testcontainers broker and is consumed back.
 *
 * <p>Follows the shared-context rule (see {@code ContainersConfig}/{@code FullContextTest}): this is
 * {@code @FullContextTest}-only, so it reuses the one shared MySQL + Redis + Kafka container set. The
 * round-trip is verified with a manually-created consumer from the auto-configured
 * {@link ConsumerFactory} rather than a {@code @KafkaListener} bean — registering a listener component
 * would alter the test-context cache key and spin up a second container set. Asserting a real
 * {@code @KafkaListener} container factory is sequenced to milestone 6 (Kafka event distribution).
 */
@FullContextTest
class KafkaTest {

    private static final String TOPIC = "skeleton-topic";
    private static final String GROUP = "skeleton-test";

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @Test
    void publishAndConsume() throws Exception {
        try (Consumer<String, String> consumer = consumerFactory.createConsumer(GROUP, null)) {
            consumer.subscribe(List.of(TOPIC));

            SendResult<String, String> sendResult = kafkaTemplate.send(TOPIC, "hello").get(10, TimeUnit.SECONDS);
            assertThat(sendResult.getRecordMetadata()).isNotNull();

            ConsumerRecord<String, String> record =
                    KafkaTestUtils.getSingleRecord(consumer, TOPIC, Duration.ofSeconds(10));
            assertThat(record).isNotNull();
            assertThat(record.value()).isEqualTo("hello");
        }
    }
}
