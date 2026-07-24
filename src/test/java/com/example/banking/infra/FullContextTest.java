package com.example.banking.infra;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The single stereotype for full-context integration tests.
 *
 * <p>Using this annotation verbatim (with no additional context-altering annotations) keeps the
 * whole test suite on one shared {@code ApplicationContext} and therefore one shared set of
 * backing-service containers from {@link ContainersConfig}. Spring's test-context cache keys a
 * context by its exact annotation set, so any test that adds or omits an annotation gets a
 * different context — and a new, separately-started MySQL container. Every full-context test must
 * use only {@code @FullContextTest}, not its own {@code @SpringBootTest}/{@code @AutoConfigureMockMvc}/
 * {@code @Import(ContainersConfig.class)} combination.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest
@AutoConfigureMockMvc
@Import(ContainersConfig.class)
public @interface FullContextTest {
}
