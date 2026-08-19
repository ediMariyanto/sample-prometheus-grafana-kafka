package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** messageId 26 - running daily statistics for an order book. */
public record MarketStatistics(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,
        long timestamp,

        long orderBookId,
        long openPrice,
        long highPrice,
        long lowPrice,
        long lastPrice,
        long lastQuantity,
        long lastAuctionPrice,
        long vwap,
        long dailyQuantity,
        long dailyValue,
        long dailyNumberOfTrades,
        long dailyTradeReportedQuantity
) implements OrderBookScoped {

    @Override
    public MessageType messageType() {
        return MessageType.MARKET_STATISTICS;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }
}
