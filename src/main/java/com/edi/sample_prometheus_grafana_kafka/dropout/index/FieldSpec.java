package com.edi.sample_prometheus_grafana_kafka.dropout.index;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketMessage;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * One field of one message structure, together with the accessor that reads it.
 *
 * <p>Derived from the record component rather than declared by hand: the 19 structures carry 316
 * fields between them, and a hand-maintained list would drift from the records the first time one
 * changed. The accessor is the record's own public accessor, resolved once at startup.
 */
public final class FieldSpec {

    /** How a field can be compared. Nested arrays and tree nodes cannot, and say so. */
    public enum Kind {
        NUMBER,
        TEXT,
        BOOLEAN,
        UNSUPPORTED
    }

    private final String name;
    private final String wireName;
    private final Kind kind;
    private final Method accessor;
    private final Class<?> declaringClass;

    FieldSpec(String name, String wireName, Kind kind, Method accessor, Class<?> declaringClass) {
        this.name = name;
        this.wireName = wireName;
        this.kind = kind;
        this.accessor = accessor;
        this.declaringClass = declaringClass;
    }

    static Kind kindOf(Class<?> type) {
        if (type == long.class || type == int.class || type == short.class || type == byte.class
                || type == Long.class || type == Integer.class || type == Short.class || type == Byte.class) {
            return Kind.NUMBER;
        }
        if (type == boolean.class || type == Boolean.class) {
            return Kind.BOOLEAN;
        }
        if (type == String.class) {
            return Kind.TEXT;
        }
        return Kind.UNSUPPORTED;
    }

    /** The Java name, e.g. {@code subsetSeqnum}. */
    public String name() {
        return name;
    }

    /** The name as it appears in the file, e.g. {@code subset_seqnum}. Equal to {@link #name()} when unaliased. */
    public String wireName() {
        return wireName;
    }

    public Kind kind() {
        return kind;
    }

    public boolean filterable() {
        return kind != Kind.UNSUPPORTED;
    }

    /** Whether this field belongs to the given message at all. */
    public boolean appliesTo(MarketMessage message) {
        return declaringClass.isInstance(message);
    }

    public Object read(MarketMessage message) {
        try {
            return accessor.invoke(message);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("cannot read " + declaringClass.getSimpleName() + "." + name, e);
        }
    }

    public long readNumber(MarketMessage message) {
        return ((Number) read(message)).longValue();
    }

    public String readText(MarketMessage message) {
        Object value = read(message);
        return value == null ? null : value.toString();
    }

    public boolean readBoolean(MarketMessage message) {
        return (Boolean) read(message);
    }
}
