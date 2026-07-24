package com.example.banking.eventsourcing.support;

import com.example.banking.eventsourcing.saga.DeadlineRequest;
import com.example.banking.eventsourcing.saga.SagaBehaviour;
import com.example.banking.eventsourcing.saga.SagaUpdate;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** Toy saga mirroring the transfer flow: request -> debit -> credit, refund on failure, timeout. */
public final class TransferLikeSaga implements SagaBehaviour<TransferLikeSaga.State> {

    public record Requested(String txId) {}
    public record Debited(String txId) {}
    public record Credited(String txId) {}
    public record CreditFailed(String txId) {}
    public record TimedOut(String txId) {}

    public record DebitCmd(String txId) {}
    public record CreditCmd(String txId) {}
    public record RefundCmd(String txId) {}

    public enum Phase { DEBITING, CREDITING, DONE }
    public record State(String txId, Phase phase) {}

    @Override public String sagaType() { return "TransferLike"; }

    @Override public Optional<String> associationKey(Object event) {
        return switch (event) {
            case Requested e -> Optional.of(e.txId());
            case Debited e -> Optional.of(e.txId());
            case Credited e -> Optional.of(e.txId());
            case CreditFailed e -> Optional.of(e.txId());
            case TimedOut e -> Optional.of(e.txId());
            default -> Optional.empty();
        };
    }

    @Override public boolean startsSaga(Object event) { return event instanceof Requested; }

    @Override public State initial(String associationKey) {
        return new State(associationKey, Phase.DEBITING);
    }

    @Override public SagaUpdate<State> react(State state, Object event) {
        return switch (event) {
            case Requested e -> SagaUpdate.of(state)
                    .withCommands(List.of(new DebitCmd(e.txId())))
                    .withSchedule(List.of(new DeadlineRequest(
                            "timeout-" + e.txId(), Duration.ofMinutes(5), new TimedOut(e.txId()))));
            case Debited e -> SagaUpdate.of(new State(state.txId(), Phase.CREDITING))
                    .withCommands(List.of(new CreditCmd(e.txId())));
            case Credited e -> SagaUpdate.of(new State(state.txId(), Phase.DONE))
                    .withCancel(List.of("timeout-" + e.txId())).asTerminal();
            case CreditFailed e -> SagaUpdate.of(new State(state.txId(), Phase.DONE))
                    .withCommands(List.of(new RefundCmd(e.txId())))
                    .withCancel(List.of("timeout-" + e.txId())).asTerminal();
            case TimedOut e -> SagaUpdate.of(new State(state.txId(), Phase.DONE))
                    .withCommands(List.of(new RefundCmd(e.txId()))).asTerminal();
            default -> SagaUpdate.of(state);
        };
    }
}
