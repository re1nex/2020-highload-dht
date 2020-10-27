package ru.mail.polis.service.re1nex;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class MergeUtils {

    private MergeUtils() {
    }

    static Response mergeGetResponses(@NotNull final List<Response> responses, final int ack) {
        int numResponses = 0;
        int numNotFoundResponses = 0;
        boolean hasTombstone = false;
        long lastGeneration = 0;
        Response last = new Response(Response.NOT_FOUND, Response.EMPTY);
        for (final Response response : responses) {
            if (response.getStatus() == 404) {
                numNotFoundResponses++;
                numResponses++;
            } else if (response.getStatus() == 200) {
                numResponses++;
                final long generation = Long.parseLong(response.getHeader(ApiController.GENERATION));
                if (lastGeneration > generation || lastGeneration == 0) {
                    lastGeneration = generation;
                    last = response;
                }
                if (response.getHeader(ApiController.TOMBSTONE) != null) {
                    hasTombstone = true;
                }
            }
        }
        if (numResponses < ack) {
            return new Response(ApiController.NOT_ENOUGH_REPLICAS, Response.EMPTY);
        }
        if (hasTombstone || numResponses == numNotFoundResponses) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return Response.ok(last.getBody());

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
