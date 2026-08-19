package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 14 - order rejected by the matching engine, carrying the engine's {@code errorCode}. */
public record OrderReject(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,

        long orderId,
        long orderBookId,
        long actorId,
        int side,
        long price,
        long quantity,
        int errorCode,
        long rejectTime
) implements OrderBookScoped {

    @Override
    public MessageType messageType() {
        return MessageType.ORDER_REJECT;
    }

    @Override
    public long timestampNanos() {
        return rejectTime;
    }
}
