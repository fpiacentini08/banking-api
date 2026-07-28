package com.example.banking.config;

import com.example.banking.domain.user.RegisterUser;
import com.example.banking.domain.user.User;
import com.example.banking.domain.user.UserCommand;
import com.example.banking.domain.user.UserEvent;
import com.example.banking.domain.user.UserRegistered;
import com.example.banking.eventsourcing.aggregate.EventSourcingRepository;
import com.example.banking.eventsourcing.command.CommandBus;
import com.example.banking.eventsourcing.event.EventTypeRegistry;
import org.springframework.beans.factory.InitializingBean;

/** Registers the User write model with the kernel on startup: its event types and its command
 *  handlers. Kept out of {@link UserWriteConfig} so the configuration only wires beans. */
public final class UserWriteModelRegistrar implements InitializingBean {

    private final EventTypeRegistry registry;
    private final CommandBus commandBus;
    private final EventSourcingRepository<User, UserCommand, UserEvent> repository;

    public UserWriteModelRegistrar(EventTypeRegistry registry, CommandBus commandBus,
                                   EventSourcingRepository<User, UserCommand, UserEvent> repository) {
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
        registry.register("UserRegistered", 1, UserRegistered.class);
    }

    private void registerCommandHandlers() {
        commandBus.register(RegisterUser.class, command -> command.userId().value(),
                command -> repository.execute(command.userId().value(), command));
    }
}
