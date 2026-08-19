package com.edi.sample_prometheus_grafana_kafka.dropout.index;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.lang.reflect.Field;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * The field layout of one message structure - what {@code messageId} actually discriminates.
 *
 * <p>Every {@link MessageType} maps to a record, and each record's components are that structure's
 * fields. Schemas are built once at class-load and cached, so a filter never reflects at query time
 * beyond invoking the resolved accessor.
 *
 * <p>Fields are addressable by their Java name ({@code subsetSeqnum}) or their name in the file
 * ({@code subset_seqnum}), both case-insensitively, so callers can use whichever they are reading.
 */
public final class MessageSchema {

    private static final Map<MessageType, MessageSchema> BY_TYPE = new EnumMap<>(MessageType.class);

    static {
        for (MessageType type : MessageType.values()) {
            BY_TYPE.put(type, new MessageSchema(type));
        }
    }

    private final MessageType type;
    private final List<FieldSpec> fields;
    private final Map<String, FieldSpec> byName;

    private MessageSchema(MessageType type) {
        this.type = type;
        Class<?> messageClass = type.messageClass();
        List<FieldSpec> specs = new ArrayList<>();
        Map<String, FieldSpec> index = new LinkedHashMap<>();

        for (RecordComponent component : messageClass.getRecordComponents()) {
            String name = component.getName();
            FieldSpec spec = new FieldSpec(
                    name,
                    wireNameOf(messageClass, component),
                    FieldSpec.kindOf(component.getType()),
                    component.getAccessor(),
                    messageClass);
            specs.add(spec);
            index.put(key(spec.name()), spec);
            index.putIfAbsent(key(spec.wireName()), spec);
        }
        this.fields = List.copyOf(specs);
        this.byName = Collections.unmodifiableMap(index);
    }

    /**
     * Jackson's {@code @JsonProperty} does not target record components, so an alias declared on a
     * component lands on the backing field. Read it from there, falling back to the Java name.
     */
    private static String wireNameOf(Class<?> messageClass, RecordComponent component) {
        try {
            Field field = messageClass.getDeclaredField(component.getName());
            JsonProperty alias = field.getAnnotation(JsonProperty.class);
            if (alias != null && !alias.value().isEmpty()) {
                return alias.value();
            }
        } catch (NoSuchFieldException e) {
            // A record component always has a backing field; fall through if that ever changes.
        }
        return component.getName();
    }

    public static MessageSchema of(MessageType type) {
        return BY_TYPE.get(type);
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    public MessageType type() {
        return type;
    }

    /** Every field of this structure, in declaration order. */
    public List<FieldSpec> fields() {
        return fields;
    }

    /** Resolves a field by Java name or wire name, case-insensitively. */
    public Optional<FieldSpec> field(String name) {
        return name == null ? Optional.empty() : Optional.ofNullable(byName.get(key(name.trim())));
    }

    /**
     * Resolves a field or explains what is available.
     *
     * @throws IllegalArgumentException when the name is unknown or the field cannot be compared
     */
    public FieldSpec require(String name) {
        FieldSpec spec = field(name).orElseThrow(() -> new IllegalArgumentException(
                "unknown field '" + name + "' for " + type.name() + "; filterable fields: " + filterableNames()));
        if (!spec.filterable()) {
            throw new IllegalArgumentException(
                    "field '" + name + "' of " + type.name() + " is a nested structure and cannot be filtered");
        }
        return spec;
    }

    public List<String> filterableNames() {
        List<String> names = new ArrayList<>();
        for (FieldSpec spec : fields) {
            if (spec.filterable()) {
                names.add(spec.name());
            }
        }
        return names;
    }
}
