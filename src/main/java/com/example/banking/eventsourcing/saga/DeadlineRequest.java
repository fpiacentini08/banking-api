package com.example.banking.eventsourcing.saga;

import java.time.Duration;

/** A timeout the saga wants delivered back to itself as `payload` after `after` elapses. */
public record DeadlineRequest(String deadlineId, Duration after, Object payload) {
}
