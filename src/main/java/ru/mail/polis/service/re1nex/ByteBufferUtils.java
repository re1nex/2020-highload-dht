package ru.mail.polis.service.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteBufferUtils {
    @NotNull
    private final byte[] emptyArray = new byte[0];

    byte[] byteBufferToByte(@NotNull final ByteBuffer result) {
        if (result.hasRemaining()) {
            final byte[] resultByteArray = new byte[result.remaining()];
            result.get(resultByteArray);
            return resultByteArray;
        } else {
            return emptyArray;
        }
    }

    @NotNull
    ByteBuffer getByteBufferKey(@NotNull final String id) {
        return ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
    }

}
