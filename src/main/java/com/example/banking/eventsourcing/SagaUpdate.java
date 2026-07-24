package com.example.banking.eventsourcing;

import java.util.List;

/** The outcome of one saga reaction: new state plus the effects to perform. */
public record SagaUpdate<S>(
        S state,
        boolean terminal,
        List<Object> commands,
        List<DeadlineRequest> schedule,
        List<String> cancelDeadlines) {

    public static <S> SagaUpdate<S> of(S state) {
        return new SagaUpdate<>(state, false, List.of(), List.of(), List.of());
    }

    public SagaUpdate<S> withCommands(List<Object> newCommands) {
        return new SagaUpdate<>(state, terminal, List.copyOf(newCommands), schedule, cancelDeadlines);
    }

    public SagaUpdate<S> withSchedule(List<DeadlineRequest> newSchedule) {
        return new SagaUpdate<>(state, terminal, commands, List.copyOf(newSchedule), cancelDeadlines);
    }

    public SagaUpdate<S> withCancel(List<String> newCancels) {
        return new SagaUpdate<>(state, terminal, commands, schedule, List.copyOf(newCancels));
    }

    public SagaUpdate<S> asTerminal() {
        return new SagaUpdate<>(state, true, commands, schedule, cancelDeadlines);
    }
}
