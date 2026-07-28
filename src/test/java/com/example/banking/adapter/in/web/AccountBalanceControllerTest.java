package com.example.banking.adapter.in.web;

import com.example.banking.adapter.out.projection.AccountBalanceRepository;
import com.example.banking.infra.FullContextTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@FullContextTest
class AccountBalanceControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired AccountBalanceRepository accounts;

    @Test
    void returnsZeroBalanceForTheOwner() throws Exception {
        String accountId = "account-balance-owner";
        String ownerId = "owner-balance-owner";
        accounts.upsert(accountId, ownerId, 0L, 0L, Instant.parse("2026-07-25T10:15:30Z"));

        mockMvc.perform(get("/accounts/{id}/balance", accountId).header("X-User-Id", ownerId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.balance").value("0.00"))
                .andExpect(jsonPath("$.currency").value("EUR"))
                .andExpect(jsonPath("$.asOf").isNotEmpty());
    }

    @Test
    void forbidsANonOwner() throws Exception {
        String accountId = "account-balance-forbidden";
        accounts.upsert(accountId, "owner-balance-forbidden", 0L, 0L, Instant.now());

        mockMvc.perform(get("/accounts/{id}/balance", accountId).header("X-User-Id", "other-balance-forbidden"))
                .andExpect(status().isForbidden());
    }

    @Test
    void notFoundForUnknownAccount() throws Exception {
        mockMvc.perform(get("/accounts/{id}/balance", "account-balance-unknown")
                        .header("X-User-Id", "owner-balance-unknown"))
                .andExpect(status().isNotFound());
    }
}
