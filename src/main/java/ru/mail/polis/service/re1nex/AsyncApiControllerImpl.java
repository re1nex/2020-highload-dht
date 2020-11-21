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

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
        if (replicaInfo.getAck() > topology.getUniqueSize()) {
            ApiUtils.sendResponse(session,
                    new Response(ApiUtils.NOT_ENOUGH_REPLICAS, Response.EMPTY)
            );
        }
        final Set<String> nodes;
        try {
            nodes = topology.severalNodesForKey(ByteBufferUtils.getByteBufferKey(id), replicaInfo.getFrom());
        } catch (NoSuchAlgorithmException e) {
            logger.error("Can't get hash", e);
            ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR);
            return;
        }
        final int ack = replicaInfo.getAck();
        switch (request.getMethod()) {
            case Request.METHOD_GET: {
                final List<CompletableFuture<ResponseBuilder>> responses = handleResponses(nodes,
                        () -> ApiUtils.get(id, dao, executor, topology),
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
                        () -> ApiUtils.delete(id, EMPTY_STRING, dao, executor),
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
                        () -> ApiUtils.put(id, request, EMPTY_STRING, dao, executor),
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
                        new Response(Response.BAD_REQUEST, Response.EMPTY)
                );
                break;
        }
    }

    void handleResponseLocal(@NotNull final String id,
                             @NotNull final HttpSession session,
                             @NotNull final Request request,
                             @Nullable final String timestamp) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                ApiUtils.sendResponse(session, ApiUtils.get(id, dao, executor, topology));
                break;
            case Request.METHOD_PUT:
                ApiUtils.sendResponse(session, ApiUtils.put(id, request, timestamp, dao, executor));
                break;
            case Request.METHOD_DELETE:
                ApiUtils.sendResponse(session, ApiUtils.delete(id, timestamp, dao, executor));
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

    private void mergeAndSendResponse(@NotNull final HttpSession session,
                                      @NotNull final List<CompletableFuture<ResponseBuilder>> responses,
                                      @NotNull final ApiUtils.MergeResponse mergeResponse,
                                      final int ack) {
        final CompletableFuture<Collection<ResponseBuilder>> completableFuture =
                MergeUtils.collateFutures(responses, ack, executor).whenCompleteAsync((res, err) -> {
                    if (err == null) {
                        ApiUtils.sendResponse(session,
                                mergeResponse.mergeResponse(res)
                        );
                    } else {
                        if (err instanceof IllegalStateException) {
                            ApiUtils.sendErrorResponse(session, Response.GATEWAY_TIMEOUT);
                        } else {
                            ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR);
                        }
                    }
                }, executor);
        if (completableFuture.isCancelled()) {
            logger.error("future was null");
        }
    }
}


