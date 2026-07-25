package com.example.banking.adapter.in.web;

import com.example.banking.adapter.out.projection.AccountBalanceRepository;
import com.example.banking.infra.FullContextTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@FullContextTest
class AccountBalanceControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired AccountBalanceRepository accounts;

    @Test
    void returnsZeroBalanceForTheOwner() throws Exception {
        String accountId = UUID.randomUUID().toString();
        String ownerId = UUID.randomUUID().toString();
        accounts.upsert(accountId, ownerId, 0L, 0L, Instant.parse("2026-07-25T10:15:30Z"));

        mockMvc.perform(get("/accounts/{id}/balance", accountId).header("X-User-Id", ownerId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.balance").value("0.00"))
                .andExpect(jsonPath("$.currency").value("EUR"))
                .andExpect(jsonPath("$.asOf").isNotEmpty());
    }

    @Test
    void forbidsANonOwner() throws Exception {
        String accountId = UUID.randomUUID().toString();
        accounts.upsert(accountId, UUID.randomUUID().toString(), 0L, 0L, Instant.now());

        mockMvc.perform(get("/accounts/{id}/balance", accountId).header("X-User-Id", UUID.randomUUID().toString()))
                .andExpect(status().isForbidden());
    }

    @Test
    void notFoundForUnknownAccount() throws Exception {
        mockMvc.perform(get("/accounts/{id}/balance", UUID.randomUUID().toString())
                        .header("X-User-Id", UUID.randomUUID().toString()))
                .andExpect(status().isNotFound());
    }
}
