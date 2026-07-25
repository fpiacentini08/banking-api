package com.example.banking.adapter.in.web;

import com.example.banking.infra.FullContextTest;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@FullContextTest
class AccountOpeningTest {

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired DSLContext dsl;
    @Autowired ObjectMapper mapper;

    @Test
    void opensAccountForARegisteredUser() throws Exception {
        String reg = mockMvc.perform(post("/users").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Ada Lovelace\",\"email\":\"ada@example.com\"}"))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
        String userStatusUrl = mapper.readTree(reg).get("statusUrl").asText();
        String userId = awaitCompletedResultUserId(userStatusUrl);
        awaitRow("users", "user_id", userId);   // COMPLETED lags the projection — await the row directly

        String open = mockMvc.perform(post("/accounts").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":\"" + userId + "\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.accountId").isNotEmpty())
                .andExpect(jsonPath("$.statusUrl").isNotEmpty())
                .andReturn().getResponse().getContentAsString();
        JsonNode accepted = mapper.readTree(open);
        String accountId = accepted.get("accountId").asText();

        awaitCompleted(accepted.get("statusUrl").asText());
        awaitRow("account_balance", "account_id", accountId);
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT owner_id, balance FROM account_balance WHERE account_id = ?", accountId);
        assertThat(row).containsEntry("owner_id", userId);
        assertThat(((Number) row.get("balance")).longValue()).isEqualTo(0L);
    }

    @Test
    void rejectsOpeningForUnknownUserWith422() throws Exception {
        mockMvc.perform(post("/accounts").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":\"" + UUID.randomUUID() + "\"}"))
                .andExpect(status().isUnprocessableEntity());
    }

    private String awaitCompletedResultUserId(String statusUrl) throws Exception {
        for (int i = 0; i < 100; i++) {
            JsonNode json = mapper.readTree(mockMvc.perform(get(statusUrl)).andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString());
            if ("COMPLETED".equals(json.path("status").asText())) {
                return json.path("resultUserId").asText();
            }
            Thread.sleep(100);
        }
        throw new AssertionError("registration did not COMPLETE");
    }

    private void awaitCompleted(String statusUrl) throws Exception {
        for (int i = 0; i < 100; i++) {
            JsonNode json = mapper.readTree(mockMvc.perform(get(statusUrl)).andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString());
            if ("COMPLETED".equals(json.path("status").asText())) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("transaction did not COMPLETE");
    }

    private void awaitRow(String tableName, String idColumn, String idValue) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            if (dsl.fetchExists(table(tableName), field(idColumn).eq(idValue))) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(tableName + " row did not appear for " + idValue);
    }
}
