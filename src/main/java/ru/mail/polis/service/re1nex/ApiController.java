package ru.mail.polis.service.re1nex;

import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import ru.mail.polis.dao.re1nex.Topology;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

abstract class ApiController {
    @NotNull
    protected final Topology<String> topology;
    @NotNull
    protected final Logger logger;

    protected ApiController(@NotNull Topology<String> topology, @NotNull Logger logger) {
        this.topology = topology;
        this.logger = logger;
    }

    void sendReplica(@NotNull final String id,
                     @NotNull final ReplicaInfo replicaInfo,
                     @NotNull final HttpSession session,
                     @NotNull final Request request) {
        final int from = replicaInfo.getFrom();
        final ByteBuffer key = ByteBufferUtils.getByteBufferKey(id);
        final Set<String> nodes;
        try {
            nodes = topology.severalNodesForKey(key, from);
        } catch (NoSuchAlgorithmException e) {
            logger.error("Can't get hash", e);
            ApiUtils.sendErrorResponse(session, Response.INTERNAL_ERROR, logger);
            return;
        }
        final int ack = replicaInfo.getAck();
        handleResponses(id, session, request, ack, nodes);
    }

    void handleResponseLocal(@NotNull final String id,
                             @NotNull final HttpSession session,
                             @NotNull final Request request) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                get(id, session);
                break;
            case Request.METHOD_PUT:
                put(id, session, request);
                break;
            case Request.METHOD_DELETE:
                delete(id, session);
                break;
            default:
                break;
        }
    }

    protected abstract void handleResponses(@NotNull final String id,
                                            @NotNull final HttpSession session,
                                            @NotNull final Request request,
                                            final int ack,
                                            @NotNull final Set<String> nodes);

    protected abstract void put(@NotNull final String id,
                                @NotNull final HttpSession session,
                                @NotNull final Request request);

    protected abstract void get(@NotNull final String id,
                                @NotNull final HttpSession session);

    protected abstract void delete(@NotNull final String id,
                                   @NotNull final HttpSession session);


}
