package com.example.banking.adapter.in.web;

import com.example.banking.application.OpenAccountGateway;
import com.example.banking.application.TransactionStatusStore;
import com.example.banking.application.UserDirectory;
import com.example.banking.domain.account.AccountId;
import com.example.banking.domain.account.OpenAccount;
import com.example.banking.domain.user.UserId;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

/** The async account-opening entry point: validates the owner exists, dispatches, returns a status resource. */
@RestController
public class AccountController {

    private static final String TYPE = "account-opening";

    private final UserDirectory userDirectory;
    private final TransactionStatusStore statusStore;
    private final OpenAccountGateway gateway;

    public AccountController(UserDirectory userDirectory, TransactionStatusStore statusStore,
                            OpenAccountGateway gateway) {
        this.userDirectory = userDirectory;
        this.statusStore = statusStore;
        this.gateway = gateway;
    }

    @PostMapping("/accounts")
    public ResponseEntity<OpenAccountResponse> open(@Valid @RequestBody OpenAccountRequest request) {
        UserId ownerId = new UserId(request.userId());
        if (!userDirectory.exists(ownerId)) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
                    "user not found: " + request.userId());
        }
        String transactionId = UUID.randomUUID().toString();
        AccountId accountId = new AccountId(UUID.randomUUID().toString());
        statusStore.insertPending(transactionId, TYPE);
        gateway.submit(transactionId, new OpenAccount(accountId, ownerId));
        return ResponseEntity.accepted()
                .body(new OpenAccountResponse(accountId.value(), "PENDING", "/transactions/" + transactionId));
    }
}
