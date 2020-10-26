package ru.mail.polis.dao.re1nex;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {
    private final long timestamp;
    private final ByteBuffer data;

    Value(final long timestamp, @Nullable final ByteBuffer data) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.data = data;
    }

    Value(final long timestamp) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.data = null;
    }

    public boolean isTombstone() {
        return data == null;
    }

    public ByteBuffer getData() {
        assert !isTombstone();
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(final @NotNull Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    public long getTimestamp() {
        return timestamp;
    }
}
