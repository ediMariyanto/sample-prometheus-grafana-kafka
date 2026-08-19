package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 10 - reference (previous close / theoretical) price for an order book. */
public record ReferencePrice(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,
        long timestamp,

        long orderBookId,
        long referencePrice,
        int referencePriceSource,
        long updatedTimestamp
) implements OrderBookScoped {

    @Override
    public MessageType messageType() {
        return MessageType.REFERENCE_PRICE;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }
}
