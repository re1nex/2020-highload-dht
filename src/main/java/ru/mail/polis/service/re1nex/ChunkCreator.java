package ru.mail.polis.service.re1nex;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

final class ChunkCreator {
    @NotNull
    static final byte[] END_CHUNK = "0\r\n\r\n".getBytes(StandardCharsets.UTF_8);
    @NotNull
    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
    @NotNull
    private static final byte[] LF = "\n".getBytes(StandardCharsets.UTF_8);

    private ChunkCreator() {
    }

    static byte[] createChunk(@NotNull final Record record) {
        final byte[] key = ByteBufferUtils.byteBufferToByte(record.getKey());
        final byte[] value = ByteBufferUtils.byteBufferToByte(record.getValue());
        final int dataLength = key.length + LF.length + value.length;
        final byte[] dataLen = Integer.toHexString(dataLength).getBytes(StandardCharsets.UTF_8);
        return ByteBufferUtils.byteBufferToByte(
                ByteBuffer.allocate(dataLen.length + CRLF.length + dataLength + CRLF.length)
                        .put(dataLen)
                        .put(CRLF)
                        .put(key)
                        .put(LF)
                        .put(value)
                        .put(CRLF)
                        .position(0));
    }
}
