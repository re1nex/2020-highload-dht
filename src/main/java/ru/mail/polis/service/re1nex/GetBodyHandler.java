package ru.mail.polis.service.re1nex;

import one.nio.http.Response;

import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;

class GetBodyHandler implements HttpResponse.BodyHandler<ResponseBuilder> {

    @Override
    public HttpResponse.BodySubscriber<ResponseBuilder> apply(HttpResponse.ResponseInfo responseInfo) {
        switch (responseInfo.statusCode()) {
            case 200: {
                final Optional<String> generation =
                        responseInfo.headers().firstValue(ApiUtils.GENERATION);
                if (generation.isEmpty()) {
                    throw new IllegalStateException("No generation in header");
                }
                if (responseInfo.headers().firstValue(ApiUtils.TOMBSTONE).isPresent()) {
                    return HttpResponse.BodySubscribers.replacing(
                            new ResponseBuilder(Response.OK, Long.parseLong(generation.get()), true
                            ));
                } else {
                    return HttpResponse.BodySubscribers.mapping(
                            HttpResponse.BodySubscribers.ofByteArray(),
                            bytes -> new ResponseBuilder(Response.OK, Long.parseLong(generation.get()), false,
                                    bytes
                            ));
                }
            }
            case 404: {
                final Optional<String> generation =
                        responseInfo.headers().firstValue(ApiUtils.GENERATION);
                if (generation.isEmpty()) {
                    throw new IllegalStateException("No generation in header");
                }
                return HttpResponse.BodySubscribers.replacing(
                        new ResponseBuilder(Response.NOT_FOUND));
            }
            default: {
                throw new RejectedExecutionException("Undefined status value");
            }
        }
    }
}
