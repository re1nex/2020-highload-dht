package ru.mail.polis.service.re1nex;

import one.nio.http.Response;

import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;

class GetBodyHandler implements HttpResponse.BodyHandler<ResponseBuilder> {

    @Override
    public HttpResponse.BodySubscriber<ResponseBuilder> apply(final HttpResponse.ResponseInfo responseInfo) {
        switch (responseInfo.statusCode()) {
            case 200: {
                final Optional<String> generation =
                        responseInfo.headers().firstValue(ApiUtils.GENERATION);
                final Optional<String> fromNode =
                        responseInfo.headers().firstValue(ApiUtils.FROM_NODE);
                if (generation.isPresent() && fromNode.isPresent()) {
                    if (responseInfo.headers().firstValue(ApiUtils.TOMBSTONE).isPresent()) {
                        return HttpResponse.BodySubscribers.replacing(
                                new ResponseBuilder(Response.OK,
                                        fromNode.get(),
                                        Long.parseLong(generation.get()),
                                        true
                                ));
                    } else {
                        return HttpResponse.BodySubscribers.mapping(
                                HttpResponse.BodySubscribers.ofByteArray(),
                                bytes -> new ResponseBuilder(Response.OK,
                                        Long.parseLong(generation.get()),
                                        bytes,
                                        fromNode.get()
                                ));
                    }
                } else {
                    throw new IllegalStateException("No generation or node in header");
                }
            }
            case 404: {
                final Optional<String> fromNode =
                        responseInfo.headers().firstValue(ApiUtils.FROM_NODE);
                if (fromNode.isPresent()) {
                    return HttpResponse.BodySubscribers.replacing(
                            new ResponseBuilder(Response.NOT_FOUND, fromNode.get()));
                } else {
                    throw new IllegalStateException("No node in header");
                }
            }
            default: {
                throw new RejectedExecutionException("Undefined status value");
            }
        }
    }
}
