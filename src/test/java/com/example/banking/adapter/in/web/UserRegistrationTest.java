package com.example.banking.adapter.in.web;

import com.example.banking.infra.FullContextTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@FullContextTest
class UserRegistrationTest {

    @Autowired MockMvc mockMvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;

    @Test
    void registersUserThroughTheAsyncWritePath() throws Exception {
        String accepted = mockMvc.perform(post("/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Ada Lovelace\",\"email\":\"ada@example.com\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.transactionId").isNotEmpty())
                .andExpect(jsonPath("$.statusUrl").isNotEmpty())
                .andReturn().getResponse().getContentAsString();
        String statusUrl = mapper.readTree(accepted).get("statusUrl").asText();

        String userId = awaitCompletedUserId(statusUrl);
        assertThat(userId).isNotBlank();

        Long events = jdbc.queryForObject(
                "SELECT COUNT(*) FROM domain_event WHERE aggregate_id = ? AND event_type = 'UserRegistered'",
                Long.class, userId);
        assertThat(events).isEqualTo(1L);

        awaitUsersRow(userId);
        Map<String, Object> row =
                jdbc.queryForMap("SELECT name, email FROM users WHERE user_id = ?", userId);
        assertThat(row).containsEntry("name", "Ada Lovelace").containsEntry("email", "ada@example.com");
    }

    @Test
    void rejectsInvalidBodyWith400() throws Exception {
        mockMvc.perform(post("/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"\",\"email\":\"not-an-email\"}"))
                .andExpect(status().isBadRequest());
    }

    private String awaitCompletedUserId(String statusUrl) throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            String body = mockMvc.perform(get(statusUrl))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            JsonNode json = mapper.readTree(body);
            if ("COMPLETED".equals(json.path("status").asText())) {
                return json.path("resultUserId").asText();
            }
            Thread.sleep(100);
        }
        throw new AssertionError("transaction did not COMPLETE within timeout");
    }

    private void awaitUsersRow(String userId) throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            if (!jdbc.queryForList("SELECT 1 FROM users WHERE user_id = ?", userId).isEmpty()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("users projection row did not appear within timeout");
    }
}
