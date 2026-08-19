package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * messageId 1 - order state. One row is emitted per change, so the last row seen for an
 * {@code orderId} is that order's current state.
 *
 * <p>{@code displayQuantity}, {@code refreshQuantity} and {@code triggerPrice} use the
 * {@link MarketValues#ABSENT} sentinel when unset.
 */
public record OrderMessage(
        long offset,
        int partitionId,
        @JsonProperty("subset_seqnum") long subsetSeqnum,
        @JsonProperty("tcp_seqnum") long tcpSeqnum,
        int messageGroup,

        long orderId,
        long previousOrderId,
        long orderBookId,
        long trackedOrderbookId,
        int side,
        long price,
        long capPrice,

        long orderQuantity,
        long originalQuantity,
        long leavesQuantity,
        long matchedQuantity,
        long minimumQuantity,
        long displayQuantity,
        long refreshQuantity,

        int orderType,
        int initialOrderType,
        int exchangeOrderType,
        int orderStatus,
        int orderStatusBefore,
        int orderCategory,
        int orderCapacity,
        int changeReason,

        int timeInForce,
        int timeInForceData,
        long timeCreated,
        long timeChanged,

        int pegType,
        long pegOffset,
        int postOnly,
        boolean minimumExecution,
        boolean reloaded,
        boolean awayMarketLocked,

        int requestedPosition,
        int orderBookPosition,
        long selfMatchPreventionKey,

        long triggerOrderBookId,
        long triggerPrice,
        int triggerCondition,
        int triggerSessionType,

        long actorId,
        long participantId,
        long submitterId,
        long onBehalfOfSubmitterId,
        long orderToken,
        String clientOrderId,
        String account,
        String customerInfo,
        String exchangeInfo
) implements OrderBookScoped {

    @Override
    public MessageType messageType() {
        return MessageType.ORDER;
    }

    @Override
    public long timestampNanos() {
        return timeChanged;
    }
}
