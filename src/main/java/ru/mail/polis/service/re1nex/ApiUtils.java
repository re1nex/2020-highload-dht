package ru.mail.polis.service.re1nex;

import one.nio.http.HttpSession;
import one.nio.http.Response;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

final class ApiUtils {
    @NonNull
    static final String RESPONSE_ERROR = "Can't send response error";
    @NotNull
    static final String NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";
    @NotNull
    static final String GENERATION = "generation";
    @NotNull
    static final String TOMBSTONE = "tombstone";
    @NotNull
    static final String PROXY_FOR = "X-Proxy-For: ";
    @NotNull
    static final String PROXY_FOR_CLIENT = "X-Proxy-For";
    @NotNull
    static final Duration TIMEOUT = Duration.ofSeconds(10);
    static final int ACCEPTED_STATUS_CODE = 202;
    static final int CREATED_STATUS_CODE = 201;

    private ApiUtils() {
    }

    static int getStatusCodeFromStatus(@NotNull final String status) {
        switch (status) {
            case Response.ACCEPTED:
                return ACCEPTED_STATUS_CODE;
            case Response.CREATED:
                return CREATED_STATUS_CODE;
            default:
                return -1;
        }
    }

    static void sendResponse(@NotNull final HttpSession session,
                             @NotNull final Response response,
                             @NotNull final Logger logger) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            logger.error("Cannot send respose", e);
        }
    }

    static void sendResponse(@NotNull final HttpSession session,
                             @NotNull final CompletableFuture<ResponseBuilder> responseCompletableFuture,
                             @NotNull final Logger logger,
                             @NotNull final ExecutorService executorService) {
        final CompletableFuture<ResponseBuilder> completableFuture
                = responseCompletableFuture.whenCompleteAsync((response, throwable) -> {
            if (throwable == null) {
                sendResponse(session, response.getResponse(), logger);
            } else {
                sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
            }
        }, executorService);
        if (completableFuture == null) {
            logger.error("future is null");
        }
    }

    static void sendErrorResponse(@NotNull final HttpSession session,
                                  @NotNull final String internalError,
                                  @NotNull final Logger logger) {
        try {
            session.sendResponse(new Response(internalError, Response.EMPTY));
        } catch (IOException ioException) {
            logger.error(RESPONSE_ERROR, ioException);
        }
    }

    @NotNull
    static HttpRequest.Builder proxyRequestBuilder(
            @NotNull final String node,
            @NotNull final String id
    ) {
        try {
            return HttpRequest.newBuilder()
                    .uri(new URI(node + "/v0/entity?id=" + id))
                    .header(PROXY_FOR_CLIENT, "True")
                    .timeout(TIMEOUT);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
