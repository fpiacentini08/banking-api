package com.example.banking.application;

import com.example.banking.domain.user.RegisterUser;
import com.example.banking.domain.user.UserId;
import com.example.banking.eventsourcing.command.CommandBus;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletionException;

/** Application command gateway for user registration: accepts, records PENDING, dispatches, and
 *  records the terminal transaction status. */
@Component
public class RegisterUserGateway {

    private static final String TYPE = "user-registration";

    private final CommandBus commandBus;
    private final TransactionStatusStore statusStore;

    public RegisterUserGateway(CommandBus commandBus, TransactionStatusStore statusStore) {
        this.commandBus = commandBus;
        this.statusStore = statusStore;
    }

    public TransactionAccepted register(String name, String email) {
        String transactionId = UUID.randomUUID().toString();
        UserId userId = new UserId(UUID.randomUUID().toString());
        statusStore.insertPending(transactionId, TYPE);
        submit(transactionId, new RegisterUser(userId, name, email));
        return new TransactionAccepted(transactionId);
    }

    void submit(String transactionId, RegisterUser command) {
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
