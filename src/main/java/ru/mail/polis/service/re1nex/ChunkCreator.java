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
        final int dataLength = key.length + LF.length + value.length;
        final byte[] dataLen = Integer.toHexString(dataLength).getBytes(StandardCharsets.UTF_8);
        final byte[] answer = new byte[dataLen.length + CRLF.length + dataLength + CRLF.length];
        int cur = dataLen.length;
        System.arraycopy(dataLen, 0, answer, 0, cur);
        System.arraycopy(CRLF, 0, answer, cur, CRLF.length);
        cur += CRLF.length;
        System.arraycopy(key, 0, answer, cur, key.length);
        cur += key.length;
        System.arraycopy(LF, 0, answer, cur, LF.length);
        cur += LF.length;
        System.arraycopy(value, 0, answer, cur, value.length);
        cur += value.length;
        System.arraycopy(CRLF, 0, answer, cur, CRLF.length);
        return answer;
    }
}
