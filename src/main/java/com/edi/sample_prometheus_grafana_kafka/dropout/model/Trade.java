package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.JsonNode;

/**
 * messageId 20 - execution. Both sides of a match share a {@link #matchId()}, so a fill is
 * reported once per participating order.
 */
public record Trade(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,

        long matchId,
        long orderId,
        long orderBookId,
        long combinationGroupId,
        int side,
        long quantity,
        long tradePrice,
        long orderPrice,
        int tradeStatus,
        int tradeReportCode,
        int passiveAggressive,
        int dealSource,
        int exchangeOrderType,
        long tradeTime,
        long reportTime,

        long actorId,
        long counterPartyActorId,
        long participantId,
        long orderToken,
        String clientOrderId,
        String account,
        String customerInfo,
        String exchangeInfo,

        boolean hasRepoInformation,
        /* Always null in the observed feed; kept as a tree node so an unexpected payload survives. */
        @JsonProperty("RepurchaseAgreementInformation") JsonNode repurchaseAgreementInformation
) implements OrderBookScoped {

    @Override
    public MessageType messageType() {
        return MessageType.TRADE;
    }

    @Override
    public long timestampNanos() {
        return tradeTime;
    }
}
