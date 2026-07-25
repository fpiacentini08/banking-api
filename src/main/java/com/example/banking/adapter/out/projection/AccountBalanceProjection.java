package com.example.banking.adapter.out.projection;

import com.example.banking.domain.account.AccountOpened;
import com.example.banking.eventsourcing.event.EventSerializer;
import com.example.banking.eventsourcing.event.StoredEvent;
import com.example.banking.eventsourcing.processor.EventHandler;

/** Seeds an account_balance row (balance 0) from AccountOpened events; delegates the write to the repo. */
public final class AccountBalanceProjection implements EventHandler {

    private static final String EVENT_TYPE = "AccountOpened";

    private final AccountBalanceRepository accounts;
    private final EventSerializer serializer;

    public AccountBalanceProjection(AccountBalanceRepository accounts, EventSerializer serializer) {
        this.accounts = accounts;
        this.serializer = serializer;
    }

    @Override
    public void handle(StoredEvent stored) {
        if (!EVENT_TYPE.equals(stored.event().eventType())) {
            return;
        }
        AccountOpened event = (AccountOpened) serializer.deserialize(stored.event());
        accounts.upsert(event.accountId().value(), event.ownerId().value(), 0L,
                stored.sequenceNr(), stored.event().occurredAt());
    }
}
