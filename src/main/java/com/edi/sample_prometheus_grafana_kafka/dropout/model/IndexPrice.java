package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 9 - index level for an index order book. */
public record IndexPrice(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,
        long timestamp,

        long orderBookId,
        long indexPrice
) implements OrderBookScoped {

    @Override
    public MessageType messageType() {
        return MessageType.INDEX_PRICE;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }
}
