package com.example.banking.adapter.out.eventstore.serialization;

import com.example.banking.eventsourcing.common.PayloadCodec;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

public final class JacksonPayloadCodec implements PayloadCodec {

    private final ObjectMapper mapper;

    public JacksonPayloadCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public String encode(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JacksonException e) {
            throw new IllegalArgumentException("cannot encode " + value.getClass().getName(), e);
        }
    }

    @Override
    public <T> T decode(String json, Class<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (JacksonException e) {
            throw new IllegalArgumentException("cannot decode into " + type.getName(), e);
        }
    }
}
