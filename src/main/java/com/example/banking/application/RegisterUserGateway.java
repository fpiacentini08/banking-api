package com.example.banking.application;

import com.example.banking.domain.shared.TransactionId;
import com.example.banking.domain.user.RegisterUser;
import com.example.banking.domain.user.UserId;
import com.example.banking.eventsourcing.command.CommandBus;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletionException;

/** Application command gateway for user registration: accepts, records PENDING, dispatches, and
 *  records the terminal transaction status. */
@Component
public class RegisterUserGateway {

    private static final String TYPE = "user-registration";

    private final CommandBus commandBus;
    private final TransactionStatusStore statusStore;
    private final UserIdGenerator userIds;
    private final TransactionIdGenerator transactionIds;

    public RegisterUserGateway(CommandBus commandBus, TransactionStatusStore statusStore,
                               UserIdGenerator userIds, TransactionIdGenerator transactionIds) {
        this.commandBus = commandBus;
        this.statusStore = statusStore;
        this.userIds = userIds;
        this.transactionIds = transactionIds;
    }

    public TransactionAccepted register(String name, String email) {
        TransactionId transactionId = transactionIds.next();
        UserId userId = userIds.next();
        statusStore.insertPending(transactionId, TYPE);
        submit(transactionId, new RegisterUser(userId, name, email));
        return new TransactionAccepted(transactionId);
    }

    void submit(TransactionId transactionId, RegisterUser command) {
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
