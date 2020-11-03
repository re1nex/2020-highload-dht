package ru.mail.polis.service.re1nex;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;
import ru.mail.polis.dao.re1nex.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

class ApiControllerImpl extends ApiController {
    @NotNull
    private final DAO dao;
    @NotNull
    private final Map<String, HttpClient> nodeToClient;

    ApiControllerImpl(@NotNull final DAO dao,
                      @NotNull final Topology<String> topology,
                      @NotNull final Map<String, HttpClient> nodeToClient,
                      @NotNull final Logger logger) {
        super(topology, logger);
        this.dao = dao;
        this.nodeToClient = nodeToClient;
    }

    @NotNull
    private Response proxy(@NotNull final String node,
                           @NotNull final Request request) {
        try {
            return nodeToClient.get(node).invoke(request);
        } catch (IOException | InterruptedException | HttpException | PoolException e) {
            logger.error(ApiUtils.RESPONSE_ERROR, e);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response get(@NotNull final String id) {
        try {
            final ByteBuffer key = ByteBufferUtils.getByteBufferKey(id);
            final Value value = dao.getValue(key);
            final Response response;
            if (value.isTombstone()) {
                response = new Response(Response.OK, Response.EMPTY);
                response.addHeader(ApiUtils.TOMBSTONE);
            } else {
                response = new Response(Response.OK, ByteBufferUtils.byteBufferToByte(value.getData()));
            }
            response.addHeader(ApiUtils.GENERATION + value.getTimestamp());
            return response;
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            logger.error("GET element " + id, e);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response put(@NotNull final String id,
                         @NotNull final Request request) {
        try {
            dao.upsert(ByteBufferUtils.getByteBufferKey(id), ByteBuffer.wrap(request.getBody()));
            return new Response(Response.CREATED, Response.EMPTY);
        } catch (IOException e) {
            logger.error("PUT failed! Cannot put the element: {}. Request size: {}. Cause: {}",
                    id, request.getBody().length, e.getCause());
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response delete(@NotNull final String id) {
        try {
            dao.remove(ByteBufferUtils.getByteBufferKey(id));
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } catch (IOException e) {
            logger.error("DELETE failed! Cannot get the element {}.\n Error: {}",
                    id, e.getMessage(), e);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    @Override
    public void handleResponseLocal(@NotNull final String id,
                                    @NotNull final HttpSession session,
                                    @NotNull final Request request) {
        switch (request.getMethod()) {
            case Request.METHOD_DELETE:
                ApiUtils.sendResponse(session, delete(id), logger);
                break;
            case Request.METHOD_PUT:
                ApiUtils.sendResponse(session, put(id, request), logger);
                break;
            case Request.METHOD_GET:
                ApiUtils.sendResponse(session, get(id), logger);
                break;
            default:
                break;
        }
    }

    @Override
    protected void handleResponses(@NotNull final String id,
                                   @NotNull final HttpSession session,
                                   @NotNull final Request request,
                                   final int ack,
                                   @NotNull final Set<String> nodes) {
        final List<Response> responses = new ArrayList<>();
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                handleResponses(nodes,
                        responses,
                        request,
                        () -> get(id));
                ApiUtils.sendResponse(session,
                        MergeUtils.mergeGetResponses(responses, ack), logger);
                break;
            case Request.METHOD_DELETE:
                handleResponses(nodes,
                        responses,
                        request,
                        () -> delete(id));
                ApiUtils.sendResponse(session,
                        MergeUtils.mergePutDeleteResponses(responses, ack, false), logger);
                break;
            case Request.METHOD_PUT:
                handleResponses(nodes,
                        responses,
                        request,
                        () -> put(id, request));
                ApiUtils.sendResponse(session,
                        MergeUtils.mergePutDeleteResponses(responses, ack, true), logger);
                break;
            default:
                break;
        }
    }

    @Override
    public void sendReplica(@NotNull final String id,
                            @NotNull final ReplicaInfo replicaInfo,
                            @NotNull final HttpSession session,
                            @NotNull final Request oldRequest) {
        final Request request = new Request(oldRequest);
        request.addHeader(ApiUtils.PROXY_FOR);
        super.sendReplica(id, replicaInfo, session, request);
    }

    private void handleResponses(@NotNull final Set<String> nodes,
                                 @NotNull final List<Response> responses,
                                 @NotNull final Request request,
                                 @NotNull final LocalResponse response) {
        if (topology.removeLocal(nodes)) {
            responses.add(response.handleLocalResponse());
        }
        for (final String node : nodes) {
            responses.add(proxy(node, request));
        }
    }

    interface LocalResponse {
        Response handleLocalResponse();
    }
}
