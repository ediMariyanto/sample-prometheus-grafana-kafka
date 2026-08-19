package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 6 - envelope-only marker emitted once, after the reference-data block. */
public record DirectoryEnd(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup
) implements MarketMessage {

    @Override
    public MessageType messageType() {
        return MessageType.DIRECTORY_END;
    }
}
