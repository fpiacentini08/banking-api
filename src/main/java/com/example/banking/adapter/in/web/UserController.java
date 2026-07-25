package com.example.banking.adapter.in.web;

import com.example.banking.application.RegisterUserGateway;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** The async user-registration entry point: delegate to the gateway, return the status resource. */
@RestController
public class UserController {

    private final RegisterUserGateway gateway;

    public UserController(RegisterUserGateway gateway) {
        this.gateway = gateway;
    }

    @PostMapping("/users")
    public ResponseEntity<RegisterUserResponse> register(@Valid @RequestBody RegisterUserRequest request) {
        return ResponseEntity.accepted()
                .body(RegisterUserResponse.from(gateway.register(request.name(), request.email())));
    }
}
