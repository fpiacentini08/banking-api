package com.example.banking.application;

import com.example.banking.domain.user.UserId;

/** Out-port for minting user identifiers, so the strategy can be specialised without touching
 *  the gateways that consume it. */
public interface UserIdGenerator {
    UserId next();
}
