package com.example.banking.adapter.in.web;

import com.example.banking.application.AccountBalanceQuery;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/** Query side: the account balance, owner-guarded by the X-User-Id header. */
@RestController
public class AccountBalanceController {

    private final AccountBalanceQuery query;

    public AccountBalanceController(AccountBalanceQuery query) {
        this.query = query;
    }

    @GetMapping("/accounts/{accountId}/balance")
    public BalanceResponse balance(@PathVariable String accountId,
                                   @RequestHeader("X-User-Id") String callerId) {
        return query.balance(accountId, callerId).fold(
                error -> { throw WebErrors.toResponseStatus(error); },
                BalanceResponse::from);
    }
}
