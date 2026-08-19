package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * messageId 3 - asset directory. One row per instrument (the ticker, e.g. {@code ATIC}), which the
 * order book rows reference by {@link #id()}.
 */
public record Asset(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,
        long timestamp,

        int action,
        long id,
        String name,
        String description,
        String extendedName,
        String assetType,
        String assetClassName,
        String assetSubClassName,
        String sectorCode,
        String isin,
        String remarks
) implements Named {

    @Override
    public MessageType messageType() {
        return MessageType.ASSET_DIRECTORY;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }
}
