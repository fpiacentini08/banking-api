package com.example.banking.infra;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.MySQLContainer;

/**
 * Shared backing-service Testcontainers for every full-context (@SpringBootTest) test.
 *
 * <p>Any test that needs to boot the full application context imports this class
 * ({@code @Import(ContainersConfig.class)}) rather than declaring its own inline container, so the
 * whole suite shares one place where backing services are wired up. Redis and Kafka containers for
 * later milestones belong here too, each as its own {@code @Bean}.
 */
@TestConfiguration(proxyBeanMethods = false)
public class ContainersConfig {

    @Bean
    @ServiceConnection
    MySQLContainer<?> mysqlContainer() {
        return new MySQLContainer<>("mysql:9.7");
    }
}
