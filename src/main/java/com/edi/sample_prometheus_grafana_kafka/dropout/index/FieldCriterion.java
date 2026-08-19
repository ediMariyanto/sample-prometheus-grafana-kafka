package com.edi.sample_prometheus_grafana_kafka.dropout.index;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketMessage;

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * A condition on one field of one message structure.
 *
 * <p>Three forms, chosen by the shape of the value:
 * <ul>
 *   <li>{@code field=value} - exact match</li>
 *   <li>{@code field=a,b,c} - matches any of the listed values</li>
 *   <li>{@code field=min..max} - inclusive range, numbers only; either end may be omitted
 *       ({@code ..500}, {@code 100..})</li>
 * </ul>
 *
 * <p>{@code ..} is the range separator rather than {@code -} so negative bounds stay unambiguous -
 * {@code errorCode=-420131} is a value, not a range.
 *
 * <p>Text matches ignore case. A criterion never matches a message that does not carry the field.
 */
public final class FieldCriterion {

    private static final String RANGE = "..";

    private final FieldSpec field;
    private final String raw;

    private final Set<Long> numbers;
    private final Long min;
    private final Long max;
    private final Set<String> texts;
    private final Boolean flag;

    private FieldCriterion(FieldSpec field, String raw, Set<Long> numbers, Long min, Long max,
                           Set<String> texts, Boolean flag) {
        this.field = field;
        this.raw = raw;
        this.numbers = numbers;
        this.min = min;
        this.max = max;
        this.texts = texts;
        this.flag = flag;
    }

    public static FieldCriterion parse(FieldSpec field, String value) {
        if (value == null) {
            throw new IllegalArgumentException("no value given for field '" + field.name() + "'");
        }
        String trimmed = value.trim();
        return switch (field.kind()) {
            case NUMBER -> parseNumber(field, trimmed);
            case TEXT -> parseText(field, trimmed);
            case BOOLEAN -> parseBoolean(field, trimmed);
            case UNSUPPORTED -> throw new IllegalArgumentException(
                    "field '" + field.name() + "' is a nested structure and cannot be filtered");
        };
    }

    private static FieldCriterion parseNumber(FieldSpec field, String value) {
        int separator = value.indexOf(RANGE);
        if (separator >= 0) {
            String lower = value.substring(0, separator).trim();
            String upper = value.substring(separator + RANGE.length()).trim();
            if (lower.isEmpty() && upper.isEmpty()) {
                throw new IllegalArgumentException("range for '" + field.name() + "' needs at least one bound");
            }
            Long from = lower.isEmpty() ? null : toLong(field, lower);
            Long to = upper.isEmpty() ? null : toLong(field, upper);
            if (from != null && to != null && from > to) {
                throw new IllegalArgumentException(
                        "range for '" + field.name() + "' is inverted: " + from + RANGE + to);
            }
            return new FieldCriterion(field, value, null, from, to, null, null);
        }
        Set<Long> values = new LinkedHashSet<>();
        for (String part : value.split(",")) {
            String token = part.trim();
            if (!token.isEmpty()) {
                values.add(toLong(field, token));
            }
        }
        if (values.isEmpty()) {
            throw new IllegalArgumentException("no value given for field '" + field.name() + "'");
        }
        return new FieldCriterion(field, value, values, null, null, null, null);
    }

    private static long toLong(FieldSpec field, String token) {
        try {
            return Long.parseLong(token);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "field '" + field.name() + "' is numeric, but got '" + token + "'", e);
        }
    }

    private static FieldCriterion parseText(FieldSpec field, String value) {
        Set<String> values = new LinkedHashSet<>();
        for (String part : value.split(",")) {
            values.add(part.trim().toLowerCase(Locale.ROOT));
        }
        return new FieldCriterion(field, value, null, null, null, values, null);
    }

    private static FieldCriterion parseBoolean(FieldSpec field, String value) {
        if (value.equalsIgnoreCase("true")) {
            return new FieldCriterion(field, value, null, null, null, null, Boolean.TRUE);
        }
        if (value.equalsIgnoreCase("false")) {
            return new FieldCriterion(field, value, null, null, null, null, Boolean.FALSE);
        }
        throw new IllegalArgumentException("field '" + field.name() + "' is a flag; use true or false, not '" + value + "'");
    }

    public FieldSpec field() {
        return field;
    }

    public String raw() {
        return raw;
    }

    public boolean test(MarketMessage message) {
        if (!field.appliesTo(message)) {
            return false; // a structure that does not carry the field cannot satisfy a condition on it
        }
        return switch (field.kind()) {
            case NUMBER -> testNumber(field.readNumber(message));
            case TEXT -> testText(field.readText(message));
            case BOOLEAN -> flag != null && flag == field.readBoolean(message);
            case UNSUPPORTED -> false;
        };
    }

    private boolean testNumber(long actual) {
        if (numbers != null) {
            return numbers.contains(actual);
        }
        return (min == null || actual >= min) && (max == null || actual <= max);
    }

    private boolean testText(String actual) {
        if (actual == null) {
            return false;
        }
        return texts.contains(actual.toLowerCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return field.name() + "=" + raw;
    }
}
