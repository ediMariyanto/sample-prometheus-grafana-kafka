package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 19 - closes the transaction opened by {@link TransactionBegin}. */
public record TransactionEnd(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,

        long transactionId,
        long transactionTime,
        long gatewayTime,
        long duration
) implements MarketMessage {

    @Override
    public MessageType messageType() {
        return MessageType.TRANSACTION_END;
    }

    @Override
    public long timestampNanos() {
        return transactionTime;
    }
}
