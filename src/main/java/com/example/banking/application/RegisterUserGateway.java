package com.example.banking.application;

import com.example.banking.domain.user.RegisterUser;
import com.example.banking.eventsourcing.command.CommandBus;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletionException;

/**
 * Application command gateway for user registration: dispatches RegisterUser asynchronously and
 * records the terminal transaction status when the command settles.
 */
@Component
public class RegisterUserGateway {

    private final CommandBus commandBus;
    private final TransactionStatusStore statusStore;

    public RegisterUserGateway(CommandBus commandBus, TransactionStatusStore statusStore) {
        this.commandBus = commandBus;
        this.statusStore = statusStore;
    }

    public void submit(String transactionId, RegisterUser command) {
        commandBus.dispatch(command).whenComplete((result, error) -> {
            if (error != null) {
                statusStore.markFailed(transactionId, describe(error));
            } else if (result.isLeft()) {
                statusStore.markRejected(transactionId, result.getLeft().message());
            } else {
                statusStore.markCompleted(transactionId, command.userId().value());
            }
        });
    }

    private static String describe(Throwable error) {
        Throwable cause = error instanceof CompletionException && error.getCause() != null
                ? error.getCause() : error;
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}
