package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 5 - actor (trader / login) directory, scoped to a {@link Participant}. */
public record Actor(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,
        long timestamp,

        int action,
        long id,
        String name,
        String fullName,
        long participantId,
        long allowedAccounts,
        boolean active,
        boolean testActor
) implements Named {

    @Override
    public MessageType messageType() {
        return MessageType.ACTOR_DIRECTORY;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }
}
