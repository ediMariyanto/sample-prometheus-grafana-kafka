package com.edi.sample_prometheus_grafana_kafka.dropout.index;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketMessage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;

import java.util.List;

/**
 * A page of filtered rows of one message type.
 *
 * <p>{@link #content()} holds the messages exactly as they were parsed, so each element serialises
 * with the field set of its own {@code messageId} - a {@code TRADE} page carries trade fields, a
 * {@code PRICE_LIMIT} page carries price-limit fields.
 *
 * <p>{@link #retained()}, {@link #matched()} and {@link #returned()} are reported separately so a
 * truncated page is never mistaken for a complete answer.
 */
public record MessagePage(
        MessageType type,
        int messageId,
        int retained,
        int matched,
        int page,
        int size,
        int returned,
        List<MarketMessage> content
) {

    public static MessagePage empty(MessageType type, int page, int size) {
        return new MessagePage(type, type.id(), 0, 0, page, size, 0, List.of());
    }
}
