package ru.mail.polis.service.re1nex;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;
import ru.mail.polis.dao.re1nex.Value;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class AsyncApiControllerImpl extends ApiController {
    @NotNull
    private final DAO dao;
    @NotNull
    private final HttpClient client;
    @NotNull
    protected final ExecutorService executor;

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

    AsyncApiControllerImpl(@NotNull final DAO dao,
                           @NotNull final Topology<String> topology,
                           @NotNull final Logger logger,
                           @NotNull final ExecutorService executor) {
        super(topology, logger);
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

    @Override
    protected void handleResponses(@NotNull final String id,
                                   @NotNull final HttpSession session,
                                   @NotNull final Request request,
                                   final int ack,
                                   @NotNull final Set<String> nodes) {
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
                        responseBuilders -> MergeUtils.mergeGetResponseBuilders(responseBuilders, ack),
                        ack);
                break;
            }
            case Request.METHOD_DELETE: {
                final List<CompletableFuture<ResponseBuilder>> responses = handleResponses(nodes,
                        () -> delete(id),
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
                        () -> put(id, request),
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

    @NotNull
    private List<CompletableFuture<ResponseBuilder>>
    handleResponses(@NotNull final Set<String> nodes,
                    @NotNull final LocalResponse response,
                    @NotNull final RequestBuilder requestBuilder,
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

    @Override
    protected void put(@NotNull final String id,
                       @NotNull final HttpSession session,
                       @NotNull final Request request) {
        ApiUtils.sendResponse(session, put(id, request), logger);
    }

    @NotNull
    private CompletableFuture<ResponseBuilder> put(@NotNull final String id,
                                                   @NotNull final Request request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                dao.upsert(ByteBufferUtils.getByteBufferKey(id), ByteBuffer.wrap(request.getBody()));
                return new ResponseBuilder(Response.CREATED);
            } catch (IOException e) {
                logger.error("PUT failed! Cannot put the element: {}. Request size: {}. Cause: {}",
                        id, request.getBody().length, e.getCause());
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    protected void get(@NotNull final String id,
                       @NotNull final HttpSession session) {
        ApiUtils.sendResponse(session, get(id), logger);
    }

    @NotNull
    private CompletableFuture<ResponseBuilder> get(@NotNull final String id) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                final ByteBuffer key = ByteBufferUtils.getByteBufferKey(id);
                final Value value = dao.getValue(key);
                if (value.isTombstone()) {
                    return new ResponseBuilder(Response.OK, value.getTimestamp(), true);
                } else {
                    return new ResponseBuilder(Response.OK, value.getTimestamp(),
                            ByteBufferUtils.byteBufferToByte(value.getData()));
                }
            } catch (NoSuchElementException e) {
                return new ResponseBuilder(Response.NOT_FOUND);
            } catch (IOException e) {
                logger.error("GET element " + id, e);
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    protected void delete(@NotNull final String id,
                          @NotNull final HttpSession session) {
        ApiUtils.sendResponse(session, delete(id), logger);
    }

    @NotNull
    private CompletableFuture<ResponseBuilder> delete(@NotNull final String id) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                dao.remove(ByteBufferUtils.getByteBufferKey(id));
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
                                      @NotNull final MergeResponse mergeResponse,
                                      final int ack) {
        final CompletableFuture<Collection<ResponseBuilder>> completableFuture =
                MergeUtils.collateFutures(responses, ack).whenCompleteAsync((res, err) -> {
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


