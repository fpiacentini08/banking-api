package com.example.banking.eventsourcing.aggregate;

import com.example.banking.eventsourcing.common.DomainError;

import io.vavr.control.Either;

import java.util.List;

/**
 * The functional aggregate contract: pure decide/evolve, no annotations, no reflection.
 *
 * @param <S> aggregate state (immutable)
 * @param <C> command supertype
 * @param <E> event supertype
 */
public interface AggregateBehaviour<S, C, E> {

    /** Logical stream-type name stored with every event (e.g. "Account"). */
    String aggregateType();

    S initial();

    S evolve(S state, E event);

    Either<DomainError, List<E>> decide(S state, C command);
}
