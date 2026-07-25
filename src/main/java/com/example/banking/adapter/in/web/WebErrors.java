package com.example.banking.adapter.in.web;

import com.example.banking.application.ApplicationError;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** The single place mapping a typed application error to an HTTP status (RFC 7807 via Spring). */
final class WebErrors {

    private WebErrors() {}

    static ResponseStatusException toResponseStatus(ApplicationError error) {
        HttpStatus status = switch (error) {
            case ApplicationError.UserNotFound ignored -> HttpStatus.UNPROCESSABLE_ENTITY;
            case ApplicationError.AccountNotFound ignored -> HttpStatus.NOT_FOUND;
            case ApplicationError.NotAccountOwner ignored -> HttpStatus.FORBIDDEN;
            case ApplicationError.TransactionNotFound ignored -> HttpStatus.NOT_FOUND;
        };
        return new ResponseStatusException(status, error.message());
    }
}
