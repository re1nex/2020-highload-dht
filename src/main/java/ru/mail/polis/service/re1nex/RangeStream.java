package ru.mail.polis.service.re1nex;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.Record;

import java.io.IOException;
import java.util.Iterator;

final class RangeStream extends HttpSession {
    @NotNull
    private static final String CHUNKED_HEADER = "Transfer-Encoding: chunked";
    @Nullable
    private Iterator<Record> iterator;

    RangeStream(@NotNull final Socket socket,
                @NotNull final HttpServer server) {
        super(socket, server);
    }

    void setIterator(@NotNull final Iterator<Record> iterator) throws IOException {
        this.iterator = iterator;
        final Response response = new Response(Response.OK);
        response.addHeader(CHUNKED_HEADER);
        writeResponse(response, false);
        next();
    }

    @Override
    public void processWrite() throws Exception {
        super.processWrite();
        next();
    }

    private void next() throws IOException {
        if (iterator == null) {
            return;
        }
        while (iterator.hasNext() && queueHead == null) {
            final Record record = iterator.next();
            final byte[] chunk = ChunkCreator.createChunk(record);
            write(chunk, 0, chunk.length);
        }
        if (iterator.hasNext()) {
            return;
        }

        write(ChunkCreator.END_CHUNK, 0, ChunkCreator.END_CHUNK.length);

        Request handling = this.handling;
        if (handling == null) {
            throw new IOException("Out of order response");
        }
        server.incRequestsProcessed();
        final String connection = handling.getHeader("Connection: ");
        final boolean keepAlive = handling.isHttp11()
                ? !"close".equalsIgnoreCase(connection)
                : "Keep-Alive".equalsIgnoreCase(connection);
        if (!keepAlive) scheduleClose();
        this.handling = handling = pipeline.pollFirst();
        if (this.handling != null) {
            if (handling == FIN) {
                scheduleClose();
            } else {
                server.handleRequest(handling, this);
            }
        }
    }
}
