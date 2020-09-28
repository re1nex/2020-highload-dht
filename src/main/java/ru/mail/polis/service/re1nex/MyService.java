package ru.mail.polis.service.re1nex;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class MyService extends HttpServer implements Service {
    @NotNull
    private final DAO dao;
    @NotNull
    private static final Logger logger = LoggerFactory.getLogger(MyService.class);

    public MyService(final int port, @NotNull final DAO dao) throws IOException {
        super(provideConfig(port));
        this.dao = dao;
    }

    private static HttpServerConfig provideConfig(final int port) {
        final AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.port = port;
        final HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        return httpServerConfig;
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok(Response.OK);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        logger.error("Unsupported mapping request.\n Cannot understand it: {} {}",
                request.getMethodName(), request.getPath());
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    /**
     * Provide request to get the value by id.
     *
     * @param id - key
     * @return 200 OK ||  400 / 404 / 500 ERROR
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_GET)
    public Response get(@Param(value = "id", required = true) final String id) {
        logger.error("GET element {}.", id);
        if (id.isEmpty()) {
            logger.debug("GET failed! Id is empty!");
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        ByteBuffer result;
        try {
            result = dao.get(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchElementException e) {
            logger.error("GET failed! Cannot find the element {}.\n Cause: {} ", id, e.getCause(), e);
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            logger.error("GET failed! Cannot get the element {}.\n Error: {}.", id, e.getMessage(), e);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        if (result.hasRemaining()) {
            final byte[] resultByteArray = new byte[result.remaining()];
            result.get(resultByteArray);
            return new Response(Response.OK, resultByteArray);
        } else {
            return new Response(Response.OK, Response.EMPTY);
        }
    }

    /**
     * Provide request to put the value by id.
     *
     * @param id      - key
     * @param request - Request with value
     * @return 201 Created || 400 / 500 ERROR
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_PUT)
    public Response put(@Param(value = "id", required = true) final String id, @NotNull final Request request) {
        logger.error("PUT element {}.", id);
        if (id.isEmpty()) {
            logger.debug("PUT failed! Id is empty!");
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        try {
            dao.upsert(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)), ByteBuffer.wrap(request.getBody()));
        } catch (IOException e) {
            logger.debug("PUT failed! Cannot put the element: {}. Request: {}. Cause: {}",
                    id, request.getBody(), e.getCause());
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.CREATED, Response.EMPTY);
    }

    /**
     * Provide request to delete the value by id.
     *
     * @param id - key
     * @return 202 Accepted ||  400 / 500 ERROR
     */
    @Path("/v0/entity")
    @RequestMethod(Request.METHOD_DELETE)
    public Response delete(@Param(value = "id", required = true) final String id) {
        logger.error("DELETE element {}.", id);
        if (id.isEmpty()) {
            logger.debug("DELETE failed! Id is empty!");
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        try {
            dao.remove(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)));
        } catch (IOException e) {
            logger.error("DELETE failed! Cannot get the element {}.\n Error: {}", id, e.getMessage(), e);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
