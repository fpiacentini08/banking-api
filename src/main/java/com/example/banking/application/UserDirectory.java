package com.example.banking.application;

import com.example.banking.domain.user.UserId;

/** Read port: has a user with this id been registered (present in the users read model)? */
public interface UserDirectory {
    boolean exists(UserId userId);
}
