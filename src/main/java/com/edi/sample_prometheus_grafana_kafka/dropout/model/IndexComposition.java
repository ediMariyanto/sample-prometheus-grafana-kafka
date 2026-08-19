package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 101 - one constituent of an index: index book, member book, and the member's weight. */
public record IndexComposition(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,
        long timestamp,

        long indexOrderBookId,
        long memberOrderBookId,
        long weight
) implements MarketMessage {

    @Override
    public MessageType messageType() {
        return MessageType.INDEX_COMPOSITION;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }
}
