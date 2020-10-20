package ru.mail.polis.dao.re1nex;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Set;

public class ModTopology implements Topology{
    private final String[] nodes;

    public ModTopology(@NotNull final Set<String> nodes,
                       @NotNull final String me){
        assert nodes.contains(me);
    }

    @Override
    public Object primaryFor(@NotNull ByteBuffer key) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @NotNull
    @Override
    public Object[] all() {
        return new Object[0];
    }
}
