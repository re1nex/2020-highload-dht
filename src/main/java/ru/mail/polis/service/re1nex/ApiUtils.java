package ru.mail.polis.service.re1nex;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;
import ru.mail.polis.dao.re1nex.Value;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.NoSuchElementException;
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
    static final String FROM_NODE = "from-node";
    @NotNull
    static final Duration TIMEOUT = Duration.ofSeconds(1);
    @NotNull
    private static final Logger logger = LoggerFactory.getLogger(ApiUtils.class);

    static final int ACCEPTED_STATUS_CODE = 202;
    static final int CREATED_STATUS_CODE = 201;

    interface LocalResponse {
        @NotNull
        CompletableFuture<ResponseBuilder> handleLocalResponse();
    }

    interface RequestBuilder {
        @NotNull
        HttpRequest requestBuild(@NotNull final String node);
    }

    interface MergeResponse {
        @NotNull
        Response mergeResponse(@NotNull final Collection<ResponseBuilder> responses);
    }

    private ApiUtils() {
    }

    static int getStatusCodeFromStatus(@NotNull final String status) throws IllegalArgumentException {
        switch (status) {
            case Response.ACCEPTED:
                return ACCEPTED_STATUS_CODE;
            case Response.CREATED:
                return CREATED_STATUS_CODE;
            default:
                throw new IllegalArgumentException("unkown status");
        }
    }

    static void sendResponse(@NotNull final HttpSession session,
                             @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            logger.error("Cannot send respose", e);
        }
    }

    static void sendResponse(@NotNull final HttpSession session,
                             @NotNull final CompletableFuture<ResponseBuilder> responseCompletableFuture) {
        final CompletableFuture<ResponseBuilder> completableFuture
                = responseCompletableFuture.whenComplete((response, throwable) -> {
            if (throwable == null) {
                sendResponse(session, response.getResponse());
            } else {
                sendErrorResponse(session, Response.INTERNAL_ERROR);
            }
        });
        if (completableFuture.isCancelled()) {
            logger.error("future is null");
        }
    }

    static void sendErrorResponse(@NotNull final HttpSession session,
                                  @NotNull final String internalError) {
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
            return baseRequestBuilder()
                    .uri(new URI(node + "/v0/entity?id=" + id));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    static HttpRequest.Builder repairRequestBuilder(
            @NotNull final String node,
            @NotNull final String id,
            final long timestamp
    ) {
        try {
            return baseRequestBuilder()
                    .uri(new URI(node + "/v0/entity?id=" + id + "&timestamp=" + timestamp));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static HttpRequest.Builder baseRequestBuilder() {
        return HttpRequest.newBuilder()
                .header(PROXY_FOR_CLIENT, "True")
                .timeout(TIMEOUT);
    }

    @NotNull
    static CompletableFuture<ResponseBuilder> put(@NotNull final String id,
                                                  @NotNull final Request request,
                                                  @Nullable final String timestamp,
                                                  @NotNull final DAO dao,
                                                  @NotNull final ExecutorService executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (timestamp == null || timestamp.isEmpty()) {
                    dao.upsert(ByteBufferUtils.getByteBufferKey(id), ByteBuffer.wrap(request.getBody()));
                } else {
                    dao.upsert(ByteBufferUtils.getByteBufferKey(id),
                            ByteBuffer.wrap(request.getBody()),
                            Long.parseLong(timestamp));
                }
                return new ResponseBuilder(Response.CREATED);
            } catch (IOException e) {
                logger.error("PUT failed! Cannot put the element: {}. Request size: {}. Cause: {}",
                        id, request.getBody().length, e.getCause());
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @NotNull
    static CompletableFuture<ResponseBuilder> get(@NotNull final String id,
                                                  @NotNull final DAO dao,
                                                  @NotNull final ExecutorService executor,
                                                  @NotNull final Topology<String> topology) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                final ByteBuffer key = ByteBufferUtils.getByteBufferKey(id);
                final Value value = dao.getValue(key);
                if (value.isTombstone()) {
                    return new ResponseBuilder(Response.OK,
                            topology.getLocal(),
                            value.getTimestamp(),
                            true);
                } else {
                    return new ResponseBuilder(Response.OK,
                            value.getTimestamp(),
                            ByteBufferUtils.byteBufferToByte(value.getData()),
                            topology.getLocal());
                }
            } catch (NoSuchElementException e) {
                return new ResponseBuilder(Response.NOT_FOUND, topology.getLocal());
            } catch (IOException e) {
                logger.error("GET element " + id, e);
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @NotNull
    static CompletableFuture<ResponseBuilder> delete(@NotNull final String id,
                                                     @Nullable final String timestamp,
                                                     @NotNull final DAO dao,
                                                     @NotNull final ExecutorService executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (timestamp == null || timestamp.isEmpty()) {
                    dao.remove(ByteBufferUtils.getByteBufferKey(id));
                } else {
                    dao.remove(ByteBufferUtils.getByteBufferKey(id), Long.parseLong(timestamp));
                }
                return new ResponseBuilder(Response.ACCEPTED);
            } catch (IOException e) {
                logger.error("DELETE failed! Cannot get the element {}.\n Error: {}",
                        id, e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }, executor);
    }
}
