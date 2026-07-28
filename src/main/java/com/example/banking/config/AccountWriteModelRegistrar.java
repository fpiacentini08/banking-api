package com.example.banking.config;

import com.example.banking.domain.account.Account;
import com.example.banking.domain.account.AccountCommand;
import com.example.banking.domain.account.AccountEvent;
import com.example.banking.domain.account.AccountOpened;
import com.example.banking.domain.account.OpenAccount;
import com.example.banking.eventsourcing.aggregate.EventSourcingRepository;
import com.example.banking.eventsourcing.command.CommandBus;
import com.example.banking.eventsourcing.event.EventTypeRegistry;
import org.springframework.beans.factory.InitializingBean;

/** Registers the Account write model with the kernel on startup: its event types and its command
 *  handlers. Kept out of {@link AccountWriteConfig} so the configuration only wires beans. */
public final class AccountWriteModelRegistrar implements InitializingBean {

    private final EventTypeRegistry registry;
    private final CommandBus commandBus;
    private final EventSourcingRepository<Account, AccountCommand, AccountEvent> repository;

    public AccountWriteModelRegistrar(EventTypeRegistry registry, CommandBus commandBus,
                                      EventSourcingRepository<Account, AccountCommand, AccountEvent> repository) {
        this.registry = registry;
        this.commandBus = commandBus;
        this.repository = repository;
    }

    @Override
    public void afterPropertiesSet() {
        registerEventTypes();
        registerCommandHandlers();
    }

    private void registerEventTypes() {
        registry.register("AccountOpened", 1, AccountOpened.class);
    }

    private void registerCommandHandlers() {
        commandBus.register(OpenAccount.class, command -> command.accountId().value(),
                command -> repository.execute(command.accountId().value(), command));
    }
}
