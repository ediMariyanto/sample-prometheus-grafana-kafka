package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 7 - auto-rejection band for an order book, relative to {@link #referencePrice()}. */
public record PriceLimit(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,
        long timestamp,

        long orderBookId,
        boolean priceLimits,
        boolean dynamic,
        long lowerLimit,
        long upperLimit,
        long referencePrice
) implements OrderBookScoped {

    @Override
    public MessageType messageType() {
        return MessageType.PRICE_LIMIT;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }
}
