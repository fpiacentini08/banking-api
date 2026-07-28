package com.example.banking.adapter.out.id;

import com.example.banking.application.AccountIdGenerator;
import com.example.banking.domain.account.AccountId;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class UuidAccountIdGenerator implements AccountIdGenerator {

    @Override
    public AccountId next() {
        return new AccountId(UUID.randomUUID().toString());
    }
}
