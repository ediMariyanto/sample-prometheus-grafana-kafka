package com.edi.sample_prometheus_grafana_kafka.dropout.model;

/**
 * Envelope shared by every row of the drop-copy file. Each concrete message is a record that
 * carries these fields plus its own payload.
 */
public interface MarketMessage {

    /** The message kind, derived from the row's {@code messageId}. */
    MessageType messageType();

    /** Byte offset of the row inside the originating feed. */
    long offset();

    int partitionId();

    long subsetSeqnum();

    long tcpSeqnum();

    int messageGroup();

    /** Epoch-nanosecond timestamp, or {@link MarketValues#ABSENT} for types that carry none. */
    default long timestampNanos() {
        return MarketValues.ABSENT;
    }
}
