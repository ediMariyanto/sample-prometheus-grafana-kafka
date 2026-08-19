package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 4 - participant (exchange member) directory. */
public record Participant(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,
        long timestamp,

        int action,
        long id,
        String name,
        String participantType,
        boolean active
) implements Named {

    @Override
    public MessageType messageType() {
        return MessageType.PARTICIPANT_DIRECTORY;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }
}
