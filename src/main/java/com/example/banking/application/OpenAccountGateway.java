package com.example.banking.application;

import com.example.banking.domain.account.AccountId;
import com.example.banking.domain.account.OpenAccount;
import com.example.banking.domain.shared.TransactionId;
import com.example.banking.domain.user.UserId;
import com.example.banking.eventsourcing.command.CommandBus;
import io.vavr.control.Either;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletionException;

/** Application command gateway for account opening: validates the owner exists, accepts, records
 *  PENDING, dispatches, and records the terminal transaction status. */
@Component
public class OpenAccountGateway {

    private static final String TYPE = "account-opening";

    private final CommandBus commandBus;
    private final TransactionStatusStore statusStore;
    private final UserDirectory userDirectory;

    public OpenAccountGateway(CommandBus commandBus, TransactionStatusStore statusStore,
                              UserDirectory userDirectory) {
        this.commandBus = commandBus;
        this.statusStore = statusStore;
        this.userDirectory = userDirectory;
    }

    public Either<ApplicationError, AccountOpeningAccepted> open(String userId) {
        UserId ownerId = new UserId(userId);
        if (!userDirectory.exists(ownerId)) {
            return Either.left(new ApplicationError.UserNotFound(userId));
        }
        TransactionId transactionId = new TransactionId(UUID.randomUUID().toString());
        AccountId accountId = new AccountId(UUID.randomUUID().toString());
        statusStore.insertPending(transactionId, TYPE);
        submit(transactionId, new OpenAccount(accountId, ownerId));
        return Either.right(new AccountOpeningAccepted(accountId, transactionId));
    }

    void submit(TransactionId transactionId, OpenAccount command) {
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
