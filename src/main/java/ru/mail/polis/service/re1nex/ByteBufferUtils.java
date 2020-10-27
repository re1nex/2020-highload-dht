package ru.mail.polis.service.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

final class ByteBufferUtils {
    @NotNull
    private static final byte[] emptyArray = new byte[0];

    private ByteBufferUtils() {
    }

    static byte[] byteBufferToByte(@NotNull final ByteBuffer result) {
        if (result.hasRemaining()) {
            final byte[] resultByteArray = new byte[result.remaining()];
            result.get(resultByteArray);
            return resultByteArray;
        } else {
            return emptyArray;
        }
    }

    @NotNull
    static ByteBuffer getByteBufferKey(@NotNull final String id) {
        return ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
    }

}
