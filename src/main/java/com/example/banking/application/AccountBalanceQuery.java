package com.example.banking.application;

import io.vavr.control.Either;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/** Query service for the account balance: owner-guarded, money rendered to a decimal string. */
@Component
public class AccountBalanceQuery {

    private final AccountBalances accountBalances;

    public AccountBalanceQuery(AccountBalances accountBalances) {
        this.accountBalances = accountBalances;
    }

    public Either<ApplicationError, AccountBalanceReadModel> balance(String accountId, String callerId) {
        return accountBalances.find(accountId)
                .<Either<ApplicationError, AccountBalanceReadModel>>map(view -> {
                    if (!view.ownerId().equals(callerId)) {
                        return Either.left(new ApplicationError.NotAccountOwner(accountId));
                    }
                    String balance = BigDecimal.valueOf(view.balanceMinor(), 2).toPlainString();
                    return Either.right(new AccountBalanceReadModel(balance, "EUR", view.updatedAt()));
                })
                .orElseGet(() -> Either.left(new ApplicationError.AccountNotFound(accountId)));
    }
}
