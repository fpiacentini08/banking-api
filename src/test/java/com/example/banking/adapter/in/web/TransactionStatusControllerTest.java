package com.example.banking.adapter.in.web;

import com.example.banking.application.TransactionStatusStore;
import com.example.banking.infra.FullContextTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@FullContextTest
class TransactionStatusControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired TransactionStatusStore store;

    @Test
    void returnsStatusForKnownTransaction() throws Exception {
        String txId = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();
        store.insertPending(txId, "user-registration");
        store.markCompleted(txId, userId);

        mockMvc.perform(get("/transactions/{id}", txId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transactionId").value(txId))
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.resultUserId").value(userId));
    }

    @Test
    void pendingStatusOmitsResultUserIdAndReason() throws Exception {
        String txId = UUID.randomUUID().toString();
        store.insertPending(txId, "user-registration");

        mockMvc.perform(get("/transactions/{id}", txId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.resultUserId").doesNotExist())
                .andExpect(jsonPath("$.reason").doesNotExist());
    }

    @Test
    void returns404ForUnknownTransaction() throws Exception {
        mockMvc.perform(get("/transactions/{id}", UUID.randomUUID().toString()))
                .andExpect(status().isNotFound());
    }
}
