package ru.mail.polis.service.re1nex;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

public interface ApiControllerFactory {
    ApiController createApiController(@NotNull final ExecutorService executors);
}
