package ru.mail.polis.service.re1nex;

import one.nio.http.Response;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;

import java.util.List;

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
                numResponses++;
            } else if (response.getStatus() == 200) {
                numResponses++;
                final long generation = Long.parseLong(response.getHeader(ApiController.GENERATION));
                if (lastGeneration < generation || lastGeneration == 0) {
                    lastGeneration = generation;
                    if (response.getHeader(ApiController.TOMBSTONE) == null) {
                        lastTombstone = false;
                        last = response;
                    } else {
                        lastTombstone = true;
                    }
                }
            }
        }
        return getResponseFromValues(numResponses,
                ack,
                lastTombstone,
                numNotFoundResponses,
                last);
    }

    static Response getResponseFromValues(final int numResponses,
                                          final int ack,
                                          final boolean lastTombstone,
                                          final int numNotFoundResponses,
                                          @NonNull final Response lastResponse) {
        if (numResponses < ack) {
            return new Response(ApiController.NOT_ENOUGH_REPLICAS, Response.EMPTY);
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
            return new Response(ApiController.NOT_ENOUGH_REPLICAS, Response.EMPTY);
        }
        return new Response(isPut ? Response.CREATED : Response.ACCEPTED, Response.EMPTY);

    }
}
