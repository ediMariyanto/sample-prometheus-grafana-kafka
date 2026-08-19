package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * messageId 18 - opens an atomic transaction; every row until the matching
 * {@link TransactionEnd} belongs to it.
 */
public record TransactionBegin(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,

        long transactionId
) implements MarketMessage {

    @Override
    public MessageType messageType() {
        return MessageType.TRANSACTION_BEGIN;
    }
}
