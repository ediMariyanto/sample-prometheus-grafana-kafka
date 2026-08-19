package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 22 - best bid / best offer snapshot for an order book. */
public record TopOfBook(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,
        long timestamp,

        long orderBookId,
        long bestBidPrice,
        long bestBidQuantity,
        long bestOfferPrice,
        long bestOfferQuantity
) implements OrderBookScoped {

    @Override
    public MessageType messageType() {
        return MessageType.TOP_OF_BOOK;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }
}
