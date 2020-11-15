package ru.mail.polis.service.re1nex;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;

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
        final byte[] data = new byte[key.length + LF.length + value.length];
        System.arraycopy(key, 0, data, 0, key.length);
        System.arraycopy(LF, 0, data, key.length, LF.length);
        System.arraycopy(value, 0, data, key.length + LF.length, value.length);
        final byte[] dataLen = Integer.toHexString(data.length).getBytes(StandardCharsets.UTF_8);
        final byte[] answer = new byte[dataLen.length + CRLF.length + data.length + CRLF.length];
        System.arraycopy(dataLen, 0, answer, 0, dataLen.length);
        System.arraycopy(CRLF, 0, answer, dataLen.length, CRLF.length);
        System.arraycopy(data, 0, answer, dataLen.length + CRLF.length, data.length);
        System.arraycopy(CRLF, 0, answer, dataLen.length + CRLF.length + data.length, CRLF.length);
        return answer;
    }
}
