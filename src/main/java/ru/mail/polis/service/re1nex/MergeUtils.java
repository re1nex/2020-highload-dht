package ru.mail.polis.service.re1nex;

import one.nio.http.Response;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

final class MergeUtils {

    private MergeUtils() {
    }

    static Response mergeGetResponses(@NotNull final List<Response> responses, final int ack) {
        int numResponses = 0;
        int numNotFoundResponses = 0;
        boolean lastTombstone = false;
        long lastGeneration = 0;
        Response last = new Response(Response.NOT_FOUND, Response.EMPTY);
        for (final Response response : responses) {
            if (response.getStatus() == 404) {
                numNotFoundResponses++;
            } else if (response.getStatus() == 200) {
                final long generation = Long.parseLong(response.getHeader(ApiUtils.GENERATION));
                if (lastGeneration < generation || lastGeneration == 0) {
                    lastGeneration = generation;
                    if (response.getHeader(ApiUtils.TOMBSTONE) == null) {
                        lastTombstone = false;
                        last = response;
                    } else {
                        lastTombstone = true;
                    }
                }
            }
            numResponses++;
        }
        return getResponseFromValues(numResponses,
                ack,
                lastTombstone,
                numNotFoundResponses,
                last);
    }

    static Response mergeGetResponseBuilders(@NotNull final Collection<ResponseBuilder> responses, final int ack) {
        int numResponses = 0;
        int numNotFoundResponses = 0;
        boolean lastTombstone = false;
        long lastGeneration = 0;
        ResponseBuilder last = new ResponseBuilder(Response.NOT_FOUND);
        for (final ResponseBuilder response : responses) {
            if (response.getStatus() == 404) {
                numNotFoundResponses++;
            } else if (response.getStatus() == 200) {
                final long generation = response.getGeneration();
                if (lastGeneration < generation || lastGeneration == 0) {
                    lastGeneration = generation;
                    if (response.isTombstone()) {
                        lastTombstone = true;
                        last = response;
                    } else {
                        lastTombstone = false;
                    }
                }
            }
            numResponses++;
        }
        return getResponseFromValues(numResponses,
                ack,
                lastTombstone,
                numNotFoundResponses,
                last.getResponse());
    }

    static Response getResponseFromValues(final int numResponses,
                                          final int ack,
                                          final boolean lastTombstone,
                                          final int numNotFoundResponses,
                                          @NonNull final Response lastResponse) {
        if (numResponses < ack) {
            return new Response(ApiUtils.NOT_ENOUGH_REPLICAS, Response.EMPTY);
        }
        if (lastTombstone || numResponses == numNotFoundResponses) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return Response.ok(lastResponse.getBody());
    }

    static Response mergePutDeleteResponses(@NotNull final List<Response> responses,
                                            final int ack,
                                            final boolean isPut) {
        int numResponses = 0;
        final int statusOk = isPut ? 201 : 202;
        for (final Response response : responses) {
            if (response.getStatus() == statusOk) {
                numResponses++;
            }
        }
        if (numResponses < ack) {
            return new Response(ApiUtils.NOT_ENOUGH_REPLICAS, Response.EMPTY);
        }
        return new Response(isPut ? Response.CREATED : Response.ACCEPTED, Response.EMPTY);
    }

    static Response mergePutDeleteResponseBuilders(@NotNull final Collection<ResponseBuilder> responses,
                                                   final int ack,
                                                   final boolean isPut) {
        int numResponses = 0;
        final int statusOk = isPut ? 201 : 202;
        for (final ResponseBuilder response : responses) {
            if (response.getStatus() == statusOk) {
                numResponses++;
            }
        }
        if (numResponses < ack) {
            return new Response(ApiUtils.NOT_ENOUGH_REPLICAS, Response.EMPTY);
        }
        return new Response(isPut ? Response.CREATED : Response.ACCEPTED, Response.EMPTY);
    }

    @NotNull
    static <T> CompletableFuture<Collection<T>> collateFutures(@NotNull final Collection<CompletableFuture<T>> futures,
                                                               final int ack) {
        final AtomicInteger counterSuccess = new AtomicInteger(ack);
        final AtomicInteger counterFails = new AtomicInteger(futures.size() - ack + 1);
        final Collection<T> results = new CopyOnWriteArrayList<>();
        final CompletableFuture<Collection<T>> result = new CompletableFuture<>();
        futures.forEach(future -> future = future.whenComplete((res, err) -> {
            if (err == null) {
                results.add(res);
                if (counterSuccess.decrementAndGet() == 0) {
                    result.complete(results);
                }
            } else {
                if (counterFails.decrementAndGet() == 0) {
                    result.completeExceptionally(err);
                }
            }
        }));
        return result;
    }

}
