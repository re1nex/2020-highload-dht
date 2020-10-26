package ru.mail.polis.service.re1nex;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.io.IOException;

public class ApiUtils {
    @NonNull
    private static final String RESPONSE_ERROR = "Can't send response error";

    void sendResponse(@NotNull final HttpSession session,
                              @NotNull final Response response,
                              @NotNull final Logger logger) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            logger.error("Cannot send respose", e);
        }
    }

    void proxy(
            @NotNull final String node,
            @NotNull final Request request,
            @NotNull final HttpSession session,
            @NotNull final HttpClient client,
            @NotNull final Logger logger) {
        try {
            request.addHeader("X-Proxy-For: " + node);
            session.sendResponse(client.invoke(request));
        } catch (IOException | InterruptedException | PoolException | HttpException e) {
            logger.error(RESPONSE_ERROR, e);
            sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
        }
    }

    void sendErrorResponse(@NotNull final HttpSession session,
                           @NotNull final String internalError,
                           @NotNull final Logger logger) {
        try {
            session.sendResponse(new Response(internalError, Response.EMPTY));
        } catch (IOException ioException) {
            logger.error(RESPONSE_ERROR, ioException);
        }
    }
}
