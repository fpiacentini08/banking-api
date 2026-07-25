package com.example.banking.adapter.in.web;

import com.example.banking.application.AccountBalanceView;
import com.example.banking.application.AccountBalances;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;

/** Query side: the account balance, owner-guarded by the X-User-Id header. */
@RestController
public class AccountBalanceController {

    private final AccountBalances accountBalances;

    public AccountBalanceController(AccountBalances accountBalances) {
        this.accountBalances = accountBalances;
    }

    @GetMapping("/accounts/{accountId}/balance")
    public BalanceResponse balance(@PathVariable String accountId,
                                   @RequestHeader("X-User-Id") String callerId) {
        AccountBalanceView view = accountBalances.find(accountId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "account not found"));
        if (!view.ownerId().equals(callerId)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "not the account owner");
        }
        String balance = BigDecimal.valueOf(view.balanceMinor(), 2).toPlainString();
        return new BalanceResponse(balance, "EUR", view.updatedAt());
    }
}
