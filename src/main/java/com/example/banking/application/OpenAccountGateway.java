package com.example.banking.application;

import com.example.banking.domain.account.OpenAccount;
import com.example.banking.eventsourcing.command.CommandBus;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletionException;

/** Application command gateway for account opening: dispatches OpenAccount asynchronously and records
 *  the terminal transaction status when the command settles. */
@Component
public class OpenAccountGateway {

    private final CommandBus commandBus;
    private final TransactionStatusStore statusStore;

    public OpenAccountGateway(CommandBus commandBus, TransactionStatusStore statusStore) {
        this.commandBus = commandBus;
        this.statusStore = statusStore;
    }

    public void submit(String transactionId, OpenAccount command) {
        commandBus.dispatch(command).whenComplete((result, error) -> {
            if (error != null) {
                statusStore.markFailed(transactionId, describe(error));
            } else if (result.isLeft()) {
                statusStore.markRejected(transactionId, result.getLeft().message());
            } else {
                statusStore.markCompleted(transactionId, null);
            }
        });
    }

    private static String describe(Throwable error) {
        Throwable cause = error instanceof CompletionException && error.getCause() != null
                ? error.getCause() : error;
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}
