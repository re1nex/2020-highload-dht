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

    abstract void handleResponseLocal(@NotNull final String id,
                                      @NotNull final HttpSession session,
                                      @NotNull final Request request);

    protected abstract void handleResponses(@NotNull final String id,
                                            @NotNull final HttpSession session,
                                            @NotNull final Request request,
                                            final int ack,
                                            @NotNull final Set<String> nodes);

}
