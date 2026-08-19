package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 11 - indicative auction (uncrossing) state for an order book. */
public record EquilibriumPrice(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,
        long timestamp,

        long orderBookId,
        int sessionId,
        long equilibriumPrice,
        long bidQuantity,
        long offerQuantity,
        long bidImbalanceQuantity,
        long offerImbalanceQuantity,
        long bestBidPrice,
        long bestBidQuantity,
        long bestOfferPrice,
        long bestOfferQuantity
) implements OrderBookScoped {

    @Override
    public MessageType messageType() {
        return MessageType.EQUILIBRIUM_PRICE;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }
}
