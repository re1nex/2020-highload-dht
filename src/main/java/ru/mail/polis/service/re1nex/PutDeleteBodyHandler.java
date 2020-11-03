package ru.mail.polis.service.re1nex;

import one.nio.http.Response;

import java.net.http.HttpResponse;
import java.util.concurrent.RejectedExecutionException;

public class PutDeleteBodyHandler implements HttpResponse.BodyHandler<ResponseBuilder> {
    @Override
    public HttpResponse.BodySubscriber<ResponseBuilder> apply(final HttpResponse.ResponseInfo responseInfo) {
        switch (responseInfo.statusCode()) {
            case 201: {
                return HttpResponse.BodySubscribers.replacing(new ResponseBuilder(Response.CREATED));
            }
            case 202: {
                return HttpResponse.BodySubscribers.replacing(new ResponseBuilder(Response.ACCEPTED));
            }
            default: {
                throw new RejectedExecutionException("Undefined status value");
            }
        }
    }
}
