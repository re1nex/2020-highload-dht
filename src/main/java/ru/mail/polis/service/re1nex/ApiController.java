package ru.mail.polis.service.re1nex;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.re1nex.Topology;
import ru.mail.polis.dao.re1nex.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

class ApiController {
    @NotNull
    static final String NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";
    @NonNull
    private static final String RESPONSE_ERROR = "Can't send response error";
    @NotNull
    static final String GENERATION = "generation";
    @NotNull
    static final String TOMBSTONE = "tombstone";
    @NotNull
    static final String PROXY_FOR = "X-Proxy-For: ";
    @NotNull
    private final DAO dao;
    @NotNull
    private final Topology<String> topology;
    @NotNull
    private final Map<String, HttpClient> nodeToClient;
    @NotNull
    private final Logger logger;

    ApiController(@NotNull final DAO dao,
                  @NotNull final Topology<String> topology,
                  @NotNull final Map<String, HttpClient> nodeToClient,
                  @NotNull final Logger logger) {
        this.dao = dao;
        this.topology = topology;
        this.nodeToClient = nodeToClient;
        this.logger = logger;
    }

    void sendResponse(@NotNull final HttpSession session,
                      @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            logger.error("Cannot send respose", e);
        }
    }

    @NotNull
    private Response proxy(@NotNull final String node,
                           @NotNull final Request request) {
        try {
            return nodeToClient.get(node).invoke(request);
        } catch (IOException | InterruptedException | HttpException | PoolException e) {
            logger.error(RESPONSE_ERROR, e);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    void proxy(
            @NotNull final String node,
            @NotNull final Request request,
            @NotNull final HttpSession session,
            @NotNull final HttpClient client) {
        try {
            request.addHeader(PROXY_FOR + node);
            sendResponse(session, client.invoke(request));
        } catch (IOException | InterruptedException | PoolException | HttpException e) {
            logger.error(RESPONSE_ERROR, e);
            sendErrorResponse(session, Response.INTERNAL_ERROR);
        }
    }

    void sendErrorResponse(@NotNull final HttpSession session,
                           @NotNull final String internalError) {
        try {
            session.sendResponse(new Response(internalError, Response.EMPTY));
        } catch (IOException ioException) {
            logger.error(RESPONSE_ERROR, ioException);
        }
    }

    private Response get(@NotNull final String id) {
        try {
            final ByteBuffer key = ByteBufferUtils.getByteBufferKey(id);
            final Value value = dao.getValue(key);
            final Response response;
            if (value.isTombstone()) {
                response = new Response(Response.OK, Response.EMPTY);
                response.addHeader(TOMBSTONE);
            } else {
                response = new Response(Response.OK, ByteBufferUtils.byteBufferToByte(value.getData()));
            }
            response.addHeader(GENERATION + value.getTimestamp());
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

    void sendProxy(@NotNull final String id,
                   @NotNull final HttpSession session,
                   @NotNull final Request request) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                sendResponse(session, get(id));
                break;
            case Request.METHOD_PUT:
                sendResponse(session, put(id, request));
                break;
            case Request.METHOD_DELETE:
                sendResponse(session, delete(id));
                break;
            default:
                break;
        }
    }

    void sendReplica(@NotNull final String id,
                     @NotNull final ReplicaInfo replicaInfo,
                     @NotNull final HttpSession session,
                     @NotNull final Request oldRequest) {
        final Request request = new Request(oldRequest);
        request.addHeader(PROXY_FOR);
        final int from = replicaInfo.getFrom();
        final ByteBuffer key = ByteBufferUtils.getByteBufferKey(id);
        final Set<String> nodes;
        try {
            nodes = topology.severalNodesForKey(key, from);
        } catch (NoSuchAlgorithmException e) {
            logger.error("Can't get hash", e);
            sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
            return;
        }
        final List<Response> responses = new ArrayList<>();
        final int ack = replicaInfo.getAck();
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                handleResponses(nodes,
                        responses,
                        request,
                        () -> get(id));
                sendResponse(session,
                        MergeUtils.mergeGetResponses(responses, ack));
                break;
            case Request.METHOD_DELETE:
                handleResponses(nodes,
                        responses,
                        request,
                        () -> delete(id));
                sendResponse(session,
                        MergeUtils.mergePutDeleteResponses(responses, ack, false));
                break;
            case Request.METHOD_PUT:
                handleResponses(nodes,
                        responses,
                        request,
                        () -> put(id, request));
                sendResponse(session,
                        MergeUtils.mergePutDeleteResponses(responses, ack, true));
                break;
            default:
                break;
        }
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
