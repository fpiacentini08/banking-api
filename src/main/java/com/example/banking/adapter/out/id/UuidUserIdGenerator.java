package com.example.banking.adapter.out.id;

import com.example.banking.application.UserIdGenerator;
import com.example.banking.domain.user.UserId;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class UuidUserIdGenerator implements UserIdGenerator {

    @Override
    public UserId next() {
        return new UserId(UUID.randomUUID().toString());
    }
}
