package com.example.banking.adapter.in.web;

import com.example.banking.application.OpenAccountGateway;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** The async account-opening entry point: delegate to the gateway, translate the outcome to HTTP. */
@RestController
public class AccountController {

    private final OpenAccountGateway gateway;

    public AccountController(OpenAccountGateway gateway) {
        this.gateway = gateway;
    }

    @PostMapping("/accounts")
    public ResponseEntity<OpenAccountResponse> open(@Valid @RequestBody OpenAccountRequest request) {
        return gateway.open(request.userId()).fold(
                error -> { throw WebErrors.toResponseStatus(error); },
                accepted -> ResponseEntity.accepted().body(OpenAccountResponse.from(accepted)));
    }
}
