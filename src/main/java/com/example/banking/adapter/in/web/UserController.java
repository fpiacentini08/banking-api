package com.example.banking.adapter.in.web;

import com.example.banking.application.RegisterUserGateway;
import com.example.banking.application.TransactionStatusStore;
import com.example.banking.domain.user.RegisterUser;
import com.example.banking.domain.user.UserId;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/** The async user-registration entry point: accepts, dispatches, and returns a status resource. */
@RestController
public class UserController {

    private static final String TYPE = "user-registration";

    private final TransactionStatusStore statusStore;
    private final RegisterUserGateway gateway;

    public UserController(TransactionStatusStore statusStore, RegisterUserGateway gateway) {
        this.statusStore = statusStore;
        this.gateway = gateway;
    }

    @PostMapping("/users")
    public ResponseEntity<RegisterUserResponse> register(@Valid @RequestBody RegisterUserRequest request) {
        String transactionId = UUID.randomUUID().toString();
        UserId userId = new UserId(UUID.randomUUID().toString());
        statusStore.insertPending(transactionId, TYPE);
        gateway.submit(transactionId, new RegisterUser(userId, request.name(), request.email()));
        return ResponseEntity.accepted()
                .body(new RegisterUserResponse(transactionId, "PENDING", "/transactions/" + transactionId));
    }
}
