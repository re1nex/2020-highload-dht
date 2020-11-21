package ru.mail.polis.service.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Provide some util's methods for bytebuffer.
 */
public final class ByteBufferUtils {
    @NotNull
    private static final byte[] emptyArray = new byte[0];

    private ByteBufferUtils() {
    }

    /**
     * Get byte array from ByteBuffer.
     */
    @NotNull
    public static byte[] byteBufferToByte(@NotNull final ByteBuffer result) {
        if (result.hasRemaining()) {
            final byte[] resultByteArray = new byte[result.remaining()];
            result.get(resultByteArray);
            return resultByteArray;
        } else {
            return emptyArray;
        }
    }

    /**
     * Get ByteBuffer from String.
     */
    @NotNull
    public static ByteBuffer getByteBufferKey(@NotNull final String id) {
        return ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
    }

}
