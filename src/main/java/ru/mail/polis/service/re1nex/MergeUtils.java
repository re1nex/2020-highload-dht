package ru.mail.polis.service.re1nex;

import one.nio.http.Response;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

final class MergeUtils {

    @NotNull
    private static final Logger logger = LoggerFactory.getLogger(MergeUtils.class);

    private MergeUtils() {
    }

    static Response mergeGetAndRepair(@NotNull final Collection<ResponseBuilder> responses,
                                      @NotNull final HttpClient client,
                                      @NotNull final String id,
                                      final int ack) {
        int numNotFoundResponses = 0;
        long lastGeneration = 0;
        ResponseBuilder last = new ResponseBuilder(Response.NOT_FOUND);
        for (final ResponseBuilder response : responses) {
            if (response.getStatusCode() == 404) {
                numNotFoundResponses++;
            } else if (response.getStatusCode() == 200) {
                final long generation = response.getGeneration();
                if (lastGeneration < generation || lastGeneration == 0) {
                    lastGeneration = generation;
                    last = response;
                }
            }
        }
        if (numNotFoundResponses != responses.size()) {
            repair(last, responses, client, id);
        }
        return getResponseFromValues(
                responses.size(),
                ack,
                numNotFoundResponses,
                last);
    }

    static void repair(@NonNull final ResponseBuilder lastResponse,
                       @NotNull final Collection<ResponseBuilder> responses,
                       @NotNull final HttpClient client,
                       @NotNull final String id) {
        try {
            if (lastResponse.isTombstone()) {
                for (final ResponseBuilder response : responses) {
                    if (response.getStatusCode() != 404 && !response.isTombstone()) {
                        repairDelete(lastResponse, response, client, id);
                    }
                }
            } else {
                for (final ResponseBuilder response : responses) {
                    if (response.getStatusCode() == 404
                            || response.isTombstone()
                            || !Arrays.equals(response.getValue(), lastResponse.getValue())) {
                        repairPut(lastResponse, response, client, id);
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            logger.error("Cannot repair replica! ", e);
        }
    }

    static void repairPut(@NonNull final ResponseBuilder lastResponse,
                          @NotNull final ResponseBuilder response,
                          @NotNull final HttpClient client,
                          @NotNull final String id) throws IOException, InterruptedException {
        client.send(ApiUtils.repairRequestBuilder(
                response.getNode(),
                id,
                lastResponse.getGeneration())
                .PUT(HttpRequest.BodyPublishers.ofByteArray(lastResponse.getValue()))
                .build(), HttpResponse.BodyHandlers.ofString());
    }

    static void repairDelete(@NonNull final ResponseBuilder lastResponse,
                             @NotNull final ResponseBuilder response,
                             @NotNull final HttpClient client,
                             @NotNull final String id) throws IOException, InterruptedException {
        client.send(ApiUtils.repairRequestBuilder(
                response.getNode(),
                id,
                lastResponse.getGeneration())
                .DELETE()
                .build(), HttpResponse.BodyHandlers.ofString());
    }

    static Response getResponseFromValues(final int numResponses,
                                          final int ack,
                                          final int numNotFoundResponses,
                                          @NonNull final ResponseBuilder lastResponse) {
        if (numResponses < ack) {
            return new Response(ApiUtils.NOT_ENOUGH_REPLICAS, Response.EMPTY);
        }
        if (lastResponse.isTombstone() || numResponses == numNotFoundResponses) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return Response.ok(Objects.requireNonNull(lastResponse.getValue()));
    }

    static Response mergePutDeleteResponseBuilders(@NotNull final Collection<ResponseBuilder> responses,
                                                   final int ack,
                                                   final String statusOk) {
        final int status;
        try {
            status = ApiUtils.getStatusCodeFromStatus(statusOk);
        } catch (IllegalArgumentException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        int numResponse = 0;
        for (final ResponseBuilder response : responses) {
            if (response.getStatusCode() == status) {
                numResponse++;
            }
        }
        if (numResponse >= ack) {
            return new Response(statusOk, Response.EMPTY);
        }
        return new Response(ApiUtils.NOT_ENOUGH_REPLICAS, Response.EMPTY);
    }

    @NotNull
    static <T> CompletableFuture<Collection<T>>
    collateFutures(@NotNull final Collection<CompletableFuture<T>> futures,
                   final int ack,
                   @NotNull final ExecutorService executorService) {
        final AtomicInteger counterSuccess = new AtomicInteger(ack);
        final AtomicInteger counterFails = new AtomicInteger(futures.size() - ack + 1);
        final Collection<T> results = new CopyOnWriteArrayList<>();
        final CompletableFuture<Collection<T>> result = new CompletableFuture<>();
        futures.forEach(future -> future = future.whenCompleteAsync((res, err) -> {
            if (err == null) {
                results.add(res);
                if (counterSuccess.decrementAndGet() == 0) {
                    result.complete(results);
                }
            } else {
                if (counterFails.decrementAndGet() == 0) {
                    result.completeExceptionally(new IllegalStateException(err));
                }
            }
        }, executorService));
        return result;
    }

}
