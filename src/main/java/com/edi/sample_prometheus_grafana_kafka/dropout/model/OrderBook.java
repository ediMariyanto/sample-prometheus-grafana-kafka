package com.edi.sample_prometheus_grafana_kafka.dropout.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.JsonNode;

import java.util.List;

/**
 * messageId 2 - order book directory. One row per tradable book; a single {@link Asset} normally
 * has several books, one per market segment (e.g. {@code ATIC_RG}, {@code ATIC_TN}, {@code ATIC_NG}).
 */
public record OrderBook(
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
        String status,
        boolean primary,
        boolean tradable,
        boolean testOrderbook,

        long assetId,
        String assetName,
        String assetType,
        String assetAdditionalInfo,
        String additionalInfo,
        String sectorCode,
        String listingBoard,

        int marketId,
        int marketSegmentId,
        String currency,
        String currencyUnit,
        int currencyRelation,

        long lotSize,
        long minimumQuantity,
        long maximumQuantity,
        long minimumValue,
        long maximumValue,

        int decimalsInPrice,
        int decimalsInQuantity,
        int decimalsInValue,
        int decimalsInContractSize,
        int decimalsInStrikePrice,
        int decimalsInPriceQuotationFactor,

        String priceType,
        long priceQuotationFactor,
        int rankingRule,
        long ipoPrice,
        long contractSize,
        long strikePrice,
        String optionType,
        String quantityExpressedIn,

        int numberOfSettlementDays,
        int numberOfItemsPriceTick,
        int numberOfItemsCombinationLeg,

        long firstTradingDate,
        long lastTradingDate,
        long expirationDate,

        boolean hasRepoOrderbook,
        /* Always null in the observed feed; kept as a tree node so an unexpected payload survives. */
        @JsonProperty("RepoOrderbook") JsonNode repoOrderbook,
        @JsonProperty("PriceTick") List<PriceTick> priceTicks,
        @JsonProperty("CombinationLeg") List<CombinationLeg> combinationLegs
) implements Named {

    @Override
    public MessageType messageType() {
        return MessageType.ORDER_BOOK_DIRECTORY;
    }

    @Override
    public long timestampNanos() {
        return timestamp;
    }

    /** Tick-size band: {@code stepSize} applies while the price is within [lowerLimit, upperLimit]. */
    public record PriceTick(long stepSize, long lowerLimit, long upperLimit) {
    }

    /** Leg of a combination order book. Always empty in the observed feed. */
    public record CombinationLeg(long orderBookId, int side, long quantity) {
    }
}
