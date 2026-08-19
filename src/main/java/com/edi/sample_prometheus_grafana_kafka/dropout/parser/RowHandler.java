package com.edi.sample_prometheus_grafana_kafka.dropout.parser;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketMessage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;

/**
 * Callback invoked by {@link DropOutRowReader} for each row of the file.
 *
 * <p>{@link #wants(MessageType)} is asked once per message type and cached, which lets a consumer
 * that only cares about some types skip binding the rest entirely - the reader then never
 * allocates an object for those rows.
 */
public interface RowHandler {

    /** Whether rows of this type should be bound to an object. Called at most once per type. */
    default boolean wants(MessageType type) {
        return true;
    }

    void onMessage(MessageType type, MarketMessage message);

    /** A row whose {@code messageId} is not in {@link MessageType}. Ignored by default. */
    default void onUnknownMessageId(int messageId, long rowNumber) {
    }

    /** A row that could not be parsed. Rethrows by default so bad input is not silently dropped. */
    default void onMalformedRow(long rowNumber, String snippet, RuntimeException cause) {
        throw cause;
    }
}
