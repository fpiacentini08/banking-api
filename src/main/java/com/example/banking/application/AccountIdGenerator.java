package com.example.banking.application;

import com.example.banking.domain.account.AccountId;

/** Out-port for minting account identifiers, so the strategy can be specialised without touching
 *  the gateways that consume it. */
public interface AccountIdGenerator {
    AccountId next();
}
