package ru.mail.polis.service.re1nex;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;
import ru.mail.polis.dao.re1nex.Value;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class AsyncApiControllerImpl {
    @NotNull
    private static final String EMPTY_STRING = "";
    @NotNull
    private final Topology<String> topology;
    @NotNull
    private final Logger logger;
    @NotNull
    private final DAO dao;
    @NotNull
    private final HttpClient client;
    @NotNull
    private final ExecutorService executor;

    AsyncApiControllerImpl(@NotNull final DAO dao,
                           @NotNull final Topology<String> topology,
                           @NotNull final Logger logger,
                           @NotNull final ExecutorService executor) {
        this.topology = topology;
        this.logger = logger;
        this.dao = dao;
        this.executor = executor;
        final Executor clientExecutor =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                        new ThreadFactoryBuilder()
                                .setUncaughtExceptionHandler((t, e) -> logger.error("Error {} in thread {}", e, t))
                                .setNameFormat("worker_%d")
                                .build());
        this.client = HttpClient.newBuilder()
                .executor(clientExecutor)
                .connectTimeout(ApiUtils.TIMEOUT)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    void sendReplica(@NotNull final String id,
                     @NotNull final ReplicaInfo replicaInfo,
                     @NotNull final HttpSession session,
                     @NotNull final Request request) {
        final int from = replicaInfo.getFrom();
        final ByteBuffer key = ByteBufferUtils.getByteBufferKey(id);
        if (replicaInfo.getAck() > topology.getUniqueSize()) {
            ApiUtils.sendResponse(session,
                    new Response(ApiUtils.NOT_ENOUGH_REPLICAS, Response.EMPTY),
                    logger);
        }
        final Set<String> nodes;
        try {
            nodes = topology.severalNodesForKey(key, from);
        } catch (NoSuchAlgorithmException e) {
            logger.error("Can't get hash", e);
            ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
            return;
        }
        final int ack = replicaInfo.getAck();
        switch (request.getMethod()) {
            case Request.METHOD_GET: {
                final List<CompletableFuture<ResponseBuilder>> responses = handleResponses(nodes,
                        () -> get(id),
                        node -> ApiUtils.proxyRequestBuilder(node, id)
                                .GET()
                                .build(),
                        new GetBodyHandler());
                mergeAndSendResponse(session,
                        responses,
                        responseBuilders -> MergeUtils.mergeGetAndRepair(responseBuilders,
                                client,
                                id,
                                executor,
                                ack),
                        ack);
                break;
            }
            case Request.METHOD_DELETE: {
                final List<CompletableFuture<ResponseBuilder>> responses = handleResponses(nodes,
                        () -> delete(id, EMPTY_STRING),
                        node -> ApiUtils.proxyRequestBuilder(node, id)
                                .DELETE()
                                .build(),
                        new PutDeleteBodyHandler());
                mergeAndSendResponse(session,
                        responses,
                        responseBuilders ->
                                MergeUtils.mergePutDeleteResponseBuilders(responseBuilders,
                                        ack,
                                        Response.ACCEPTED
                                ),
                        ack);
                break;
            }
            case Request.METHOD_PUT: {
                final List<CompletableFuture<ResponseBuilder>> responses = handleResponses(nodes,
                        () -> put(id, request, EMPTY_STRING),
                        node -> ApiUtils.proxyRequestBuilder(node, id)
                                .PUT(HttpRequest.BodyPublishers.ofByteArray(request.getBody()))
                                .build(),
                        new PutDeleteBodyHandler());
                mergeAndSendResponse(session,
                        responses,
                        responseBuilders ->
                                MergeUtils.mergePutDeleteResponseBuilders(responseBuilders,
                                        ack,
                                        Response.CREATED
                                ),
                        ack);
                break;
            }
            default:
                ApiUtils.sendResponse(session,
                        new Response(Response.BAD_REQUEST, Response.EMPTY),
                        logger);
                break;
        }
    }

    void handleResponseLocal(@NotNull final String id,
                             @NotNull final HttpSession session,
                             @NotNull final Request request,
                             @Nullable final String timestamp) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                ApiUtils.sendResponse(session, get(id), logger);
                break;
            case Request.METHOD_PUT:
                ApiUtils.sendResponse(session, put(id, request, timestamp), logger);
                break;
            case Request.METHOD_DELETE:
                ApiUtils.sendResponse(session, delete(id, timestamp), logger);
                break;
            default:
                break;
        }
    }

    @NotNull
    private List<CompletableFuture<ResponseBuilder>>
    handleResponses(@NotNull final Set<String> nodes,
                    @NotNull final ApiUtils.LocalResponse response,
                    @NotNull final ApiUtils.RequestBuilder requestBuilder,
                    @NotNull final HttpResponse.BodyHandler<ResponseBuilder> handler) {
        final List<CompletableFuture<ResponseBuilder>> responses = new ArrayList<>();
        if (topology.removeLocal(nodes)) {
            responses.add(response.handleLocalResponse());
        }
        for (final String node : nodes) {
            final HttpRequest request = requestBuilder.requestBuild(node);
            final CompletableFuture<ResponseBuilder> responseCompletableFuture =
                    client.sendAsync(request, handler)
                            .thenApplyAsync(HttpResponse::body, executor);
            responses.add(responseCompletableFuture);
        }
        return responses;
    }

    @NotNull
    private CompletableFuture<ResponseBuilder> put(@NotNull final String id,
                                                   @NotNull final Request request,
                                                   @Nullable final String timestamp) {
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
    private CompletableFuture<ResponseBuilder> get(@NotNull final String id) {
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
    private CompletableFuture<ResponseBuilder> delete(@NotNull final String id,
                                                      @Nullable final String timestamp) {
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

    private void mergeAndSendResponse(@NotNull final HttpSession session,
                                      @NotNull final List<CompletableFuture<ResponseBuilder>> responses,
                                      @NotNull final ApiUtils.MergeResponse mergeResponse,
                                      final int ack) {
        final CompletableFuture<Collection<ResponseBuilder>> completableFuture =
                MergeUtils.collateFutures(responses, ack, executor).whenCompleteAsync((res, err) -> {
                    if (err == null) {
                        ApiUtils.sendResponse(session,
                                mergeResponse.mergeResponse(res),
                                logger);
                    } else {
                        if (err instanceof IllegalStateException) {
                            ApiUtils.sendErrorResponse(session, Response.GATEWAY_TIMEOUT, logger);
                        } else {
                            ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
                        }
                    }
                }, executor);
        if (completableFuture.isCancelled()) {
            logger.error("future was null");
        }
    }
}


