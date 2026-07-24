package com.example.banking.adapter.out.eventstore;

import com.example.banking.eventsourcing.EventTypeRegistry;
import com.example.banking.eventsourcing.SerializedEvent;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class JacksonEventSerializerTest {

    record MoneyDepositedV2(String accountId, long amountCents, String source) {}

    private final ObjectMapper mapper = new ObjectMapper();
    private final Clock clock = Clock.fixed(Instant.parse("2026-07-24T10:00:00Z"), ZoneOffset.UTC);

    private EventTypeRegistry registry() {
        EventTypeRegistry registry = new EventTypeRegistry();
        registry.register("MoneyDeposited", 2, MoneyDepositedV2.class);
        return registry;
    }

    @Test
    void roundTripsAnEventAtCurrentRevision() {
        JacksonEventSerializer serializer =
                new JacksonEventSerializer(mapper, registry(), new UpcasterChain(List.of()), clock);
        MoneyDepositedV2 event = new MoneyDepositedV2("a-1", 500, "cash");

        SerializedEvent serialized = serializer.serialize(event, Map.of("userId", "u-1"));

        assertThat(serialized.eventType()).isEqualTo("MoneyDeposited");
        assertThat(serialized.revision()).isEqualTo(2);
        assertThat(serialized.occurredAt()).isEqualTo(Instant.parse("2026-07-24T10:00:00Z"));
        assertThat(serialized.metadata()).contains("\"userId\":\"u-1\"");
        assertThat(serializer.deserialize(serialized)).isEqualTo(event);
    }

    @Test
    void upcastsAnOldRevisionOnRead() {
        Upcaster v1ToV2 = new Upcaster() {
            @Override public String eventType() { return "MoneyDeposited"; }
            @Override public int fromRevision() { return 1; }
            @Override public JsonNode upcast(JsonNode payload) {
                ((ObjectNode) payload).put("source", "unknown");
                return payload;
            }
        };
        JacksonEventSerializer serializer =
                new JacksonEventSerializer(mapper, registry(), new UpcasterChain(List.of(v1ToV2)), clock);
        SerializedEvent v1 = new SerializedEvent("e-1", "MoneyDeposited", 1,
                "{\"accountId\":\"a-1\",\"amountCents\":500}", "{}", Instant.parse("2026-01-01T00:00:00Z"));

        Object event = serializer.deserialize(v1);

        assertThat(event).isEqualTo(new MoneyDepositedV2("a-1", 500, "unknown"));
    }

    @Test
    void unregisteredTypeIsRejectedOnBothArms() {
        JacksonEventSerializer serializer =
                new JacksonEventSerializer(mapper, registry(), new UpcasterChain(List.of()), clock);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> serializer.serialize("not registered", Map.of()));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> serializer.deserialize(new SerializedEvent(
                        "e-2", "Unknown", 1, "{}", "{}", Instant.now())));
    }
}
