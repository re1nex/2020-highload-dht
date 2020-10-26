package ru.mail.polis.service.re1nex;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class MergeUtils {

    Response mergeGetResponses(@NotNull final List<Response> responses, final int ack) {
        int numResponses = 0;
        int numNotFoundResponses = 0;
        boolean hasTombstone = false;
        long lastVersion = 0;
        Response last = new Response(Response.NOT_FOUND, Response.EMPTY);
        for (Response response : responses) {
            if (response.getStatus() == 404) {
                numNotFoundResponses++;
                numResponses++;
            } else if (response.getStatus() == 200) {
                numResponses++;
                long version = Long.parseLong(response.getHeader(ApiUtils.GENERATION));
                if (lastVersion > version || lastVersion == 0) {
                    lastVersion = version;
                    last = response;
                }
                if (response.getHeader(ApiUtils.TOMBSTONE) != null) {
                    hasTombstone = true;
                }
            }
        }
        if (numResponses < ack) {
            return new Response(ApiUtils.NOT_ENOUGH_REPLICAS, Response.EMPTY);
        }
        if (hasTombstone || numResponses == numNotFoundResponses) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return Response.ok(last.getBody());

    }

    Response mergePutResponses(@NotNull final List<Response> responses, final int ack) {
        int numResponses = 0;
        for (Response response : responses) {
            if (response.getStatus() == 201) {
                numResponses++;
            }
        }
        if (numResponses < ack) {
            return new Response(ApiUtils.NOT_ENOUGH_REPLICAS, Response.EMPTY);
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    Response mergeDeleteResponses(@NotNull final List<Response> responses, final int ack) {
        int numResponses = 0;
        for (Response response : responses) {
            if (response.getStatus() == 202) {
                numResponses++;
            }
        }
        if (numResponses < ack) {
            return new Response(ApiUtils.NOT_ENOUGH_REPLICAS, Response.EMPTY);
        }
        return new Response(Response.ACCEPTED, Response.EMPTY);

    }
}
