package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * messageId 17 - the trading day this file belongs to, as epoch nanoseconds at UTC midnight.
 *
 * <p>This is a <em>label</em> for the trading day, not a bound on the timestamps in the file. The
 * exchange runs at UTC+7, so a session that opens at 09:00 local is stamped 02:00Z on the same day,
 * while pre-open and reference-data rows are stamped in the late evening of the <em>previous</em> UTC
 * day. Filtering events from {@code businessDate} onwards therefore drops the whole pre-open block;
 * derive a real instant for the window instead.
 */
public record BusinessDate(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,

        long businessDate
) implements MarketMessage {

    @Override
    public MessageType messageType() {
        return MessageType.BUSINESS_DATE;
    }

    @Override
    public long timestampNanos() {
        return businessDate;
    }
}
