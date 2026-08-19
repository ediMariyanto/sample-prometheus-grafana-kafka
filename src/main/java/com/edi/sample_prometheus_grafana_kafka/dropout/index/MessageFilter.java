package com.edi.sample_prometheus_grafana_kafka.dropout.index;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.Asset;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.EquilibriumPrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.IndexComposition;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.IndexPrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketMessage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketStatistics;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketValues;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Named;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderBook;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderBookScoped;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderMessage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderReject;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Participant;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.ReferencePrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Trade;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.TransactionBegin;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.TransactionEnd;

import java.util.List;
import java.util.Set;

/**
 * Criteria applied to the rows of one message type.
 *
 * <p>Every criterion is optional and they combine with AND. A criterion that a given message type
 * simply does not have excludes that type's rows rather than silently passing them - filtering
 * {@code PRICE_LIMIT} by {@code orderId} yields nothing, because a price limit has no order.
 *
 * <p>Envelope criteria ({@code partitionId}, sequence and timestamp ranges) apply to every type
 * through {@link MarketMessage}. Entity criteria are resolved per type; where a concept has a
 * different field name per message - a trade's price is {@code tradePrice}, an order's is
 * {@code price} - it is mapped here so callers use one name.
 */
public final class MessageFilter {

    public static final MessageFilter NONE = builder().build();

    private final Set<Long> orderBookIds;
    private final String name;
    private final String isin;
    private final Long assetId;
    private final Long orderId;
    private final Long matchId;
    private final Long transactionId;
    private final Long actorId;
    private final Long participantId;
    private final Integer side;
    private final Long minPrice;
    private final Long maxPrice;
    private final Long minQuantity;
    private final Long maxQuantity;
    private final Long fromTimestamp;
    private final Long toTimestamp;
    private final Integer partitionId;
    private final Long minSeqnum;
    private final Long maxSeqnum;
    private final List<FieldCriterion> fieldCriteria;

    private final boolean empty;

    private MessageFilter(Builder builder) {
        this.orderBookIds = builder.orderBookIds;
        this.name = builder.name;
        this.isin = builder.isin;
        this.assetId = builder.assetId;
        this.orderId = builder.orderId;
        this.matchId = builder.matchId;
        this.transactionId = builder.transactionId;
        this.actorId = builder.actorId;
        this.participantId = builder.participantId;
        this.side = builder.side;
        this.minPrice = builder.minPrice;
        this.maxPrice = builder.maxPrice;
        this.minQuantity = builder.minQuantity;
        this.maxQuantity = builder.maxQuantity;
        this.fromTimestamp = builder.fromTimestamp;
        this.toTimestamp = builder.toTimestamp;
        this.partitionId = builder.partitionId;
        this.minSeqnum = builder.minSeqnum;
        this.maxSeqnum = builder.maxSeqnum;
        this.fieldCriteria = List.copyOf(builder.fieldCriteria);
        this.empty = orderBookIds.isEmpty() && name == null && isin == null && assetId == null
                && orderId == null && matchId == null && transactionId == null && actorId == null
                && participantId == null && side == null && minPrice == null && maxPrice == null
                && minQuantity == null && maxQuantity == null && fromTimestamp == null
                && toTimestamp == null && partitionId == null && minSeqnum == null && maxSeqnum == null
                && fieldCriteria.isEmpty();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Whether no criterion was set, in which case every row matches. */
    public boolean isEmpty() {
        return empty;
    }

    public boolean test(MarketMessage message) {
        if (empty) {
            return true;
        }
        return matchesEnvelope(message)
                && matchesOrderBook(message)
                && matchesNameAndIsin(message)
                && matchesIdentifiers(message)
                && matchesPriceAndQuantity(message)
                && matchesFields(message);
    }

    /** Conditions on the structure's own fields, e.g. {@code sectorCode} or {@code errorCode}. */
    private boolean matchesFields(MarketMessage message) {
        for (FieldCriterion criterion : fieldCriteria) {
            if (!criterion.test(message)) {
                return false;
            }
        }
        return true;
    }

    /** The per-field conditions carried by this filter, for echoing back to a caller. */
    public List<FieldCriterion> fieldCriteria() {
        return fieldCriteria;
    }

    private boolean matchesEnvelope(MarketMessage message) {
        if (partitionId != null && message.partitionId() != partitionId) {
            return false;
        }
        if (minSeqnum != null && message.subsetSeqnum() < minSeqnum) {
            return false;
        }
        if (maxSeqnum != null && message.subsetSeqnum() > maxSeqnum) {
            return false;
        }
        if (fromTimestamp == null && toTimestamp == null) {
            return true;
        }
        long timestamp = message.timestampNanos();
        if (!MarketValues.isPresent(timestamp) || timestamp == 0L) {
            return false; // the type carries no usable clock, so a time window cannot include it
        }
        return (fromTimestamp == null || timestamp >= fromTimestamp)
                && (toTimestamp == null || timestamp <= toTimestamp);
    }

    private boolean matchesOrderBook(MarketMessage message) {
        if (orderBookIds.isEmpty()) {
            return true;
        }
        if (message instanceof OrderBookScoped scoped) {
            return orderBookIds.contains(scoped.orderBookId());
        }
        if (message instanceof OrderBook book) {
            return orderBookIds.contains(book.id());
        }
        if (message instanceof IndexComposition member) {
            return orderBookIds.contains(member.indexOrderBookId())
                    || orderBookIds.contains(member.memberOrderBookId());
        }
        return false;
    }

    private boolean matchesNameAndIsin(MarketMessage message) {
        if (name != null) {
            if (!(message instanceof Named named) || !name.equalsIgnoreCase(named.name())) {
                return false;
            }
        }
        if (isin != null) {
            return message instanceof Asset asset && isin.equalsIgnoreCase(asset.isin());
        }
        return true;
    }

    private boolean matchesIdentifiers(MarketMessage message) {
        if (assetId != null && assetIdOf(message) != assetId) {
            return false;
        }
        if (orderId != null && orderIdOf(message) != orderId) {
            return false;
        }
        if (matchId != null && !(message instanceof Trade trade && trade.matchId() == matchId)) {
            return false;
        }
        if (transactionId != null && transactionIdOf(message) != transactionId) {
            return false;
        }
        if (actorId != null && actorIdOf(message) != actorId) {
            return false;
        }
        if (participantId != null && participantIdOf(message) != participantId) {
            return false;
        }
        return side == null || sideOf(message) == side;
    }

    private boolean matchesPriceAndQuantity(MarketMessage message) {
        if (minPrice != null || maxPrice != null) {
            long price = priceOf(message);
            if (!MarketValues.isPresent(price)
                    || (minPrice != null && price < minPrice)
                    || (maxPrice != null && price > maxPrice)) {
                return false;
            }
        }
        if (minQuantity != null || maxQuantity != null) {
            long quantity = quantityOf(message);
            return MarketValues.isPresent(quantity)
                    && (minQuantity == null || quantity >= minQuantity)
                    && (maxQuantity == null || quantity <= maxQuantity);
        }
        return true;
    }

    // --- per-type field mapping; ABSENT means "this type has no such field" ---

    private static long assetIdOf(MarketMessage message) {
        if (message instanceof OrderBook book) {
            return book.assetId();
        }
        if (message instanceof Asset asset) {
            return asset.id();
        }
        return MarketValues.ABSENT;
    }

    private static long orderIdOf(MarketMessage message) {
        if (message instanceof OrderMessage order) {
            return order.orderId();
        }
        if (message instanceof Trade trade) {
            return trade.orderId();
        }
        if (message instanceof OrderReject reject) {
            return reject.orderId();
        }
        return MarketValues.ABSENT;
    }

    private static long transactionIdOf(MarketMessage message) {
        if (message instanceof TransactionBegin begin) {
            return begin.transactionId();
        }
        if (message instanceof TransactionEnd end) {
            return end.transactionId();
        }
        return MarketValues.ABSENT;
    }

    private static long actorIdOf(MarketMessage message) {
        if (message instanceof OrderMessage order) {
            return order.actorId();
        }
        if (message instanceof Trade trade) {
            return trade.actorId();
        }
        if (message instanceof OrderReject reject) {
            return reject.actorId();
        }
        return MarketValues.ABSENT;
    }

    private static long participantIdOf(MarketMessage message) {
        if (message instanceof OrderMessage order) {
            return order.participantId();
        }
        if (message instanceof Trade trade) {
            return trade.participantId();
        }
        if (message instanceof Participant participant) {
            return participant.id();
        }
        if (message instanceof com.edi.sample_prometheus_grafana_kafka.dropout.model.Actor actor) {
            return actor.participantId();
        }
        return MarketValues.ABSENT;
    }

    private static int sideOf(MarketMessage message) {
        if (message instanceof OrderMessage order) {
            return order.side();
        }
        if (message instanceof Trade trade) {
            return trade.side();
        }
        if (message instanceof OrderReject reject) {
            return reject.side();
        }
        return Integer.MIN_VALUE;
    }

    private static long priceOf(MarketMessage message) {
        if (message instanceof OrderMessage order) {
            return order.price();
        }
        if (message instanceof Trade trade) {
            return trade.tradePrice();
        }
        if (message instanceof OrderReject reject) {
            return reject.price();
        }
        if (message instanceof ReferencePrice reference) {
            return reference.referencePrice();
        }
        if (message instanceof IndexPrice index) {
            return index.indexPrice();
        }
        if (message instanceof EquilibriumPrice equilibrium) {
            return equilibrium.equilibriumPrice();
        }
        if (message instanceof MarketStatistics statistics) {
            return statistics.lastPrice();
        }
        return MarketValues.ABSENT;
    }

    private static long quantityOf(MarketMessage message) {
        if (message instanceof OrderMessage order) {
            return order.orderQuantity();
        }
        if (message instanceof Trade trade) {
            return trade.quantity();
        }
        if (message instanceof OrderReject reject) {
            return reject.quantity();
        }
        return MarketValues.ABSENT;
    }

    public static final class Builder {

        private Set<Long> orderBookIds = Set.of();
        private String name;
        private String isin;
        private Long assetId;
        private Long orderId;
        private Long matchId;
        private Long transactionId;
        private Long actorId;
        private Long participantId;
        private Integer side;
        private Long minPrice;
        private Long maxPrice;
        private Long minQuantity;
        private Long maxQuantity;
        private Long fromTimestamp;
        private Long toTimestamp;
        private Integer partitionId;
        private Long minSeqnum;
        private Long maxSeqnum;
        private final List<FieldCriterion> fieldCriteria = new java.util.ArrayList<>();

        private Builder() {
        }

        public Builder orderBookIds(Set<Long> orderBookIds) {
            this.orderBookIds = orderBookIds == null ? Set.of() : Set.copyOf(orderBookIds);
            return this;
        }

        public Builder name(String name) {
            this.name = blankToNull(name);
            return this;
        }

        public Builder isin(String isin) {
            this.isin = blankToNull(isin);
            return this;
        }

        public Builder assetId(Long assetId) {
            this.assetId = assetId;
            return this;
        }

        public Builder orderId(Long orderId) {
            this.orderId = orderId;
            return this;
        }

        public Builder matchId(Long matchId) {
            this.matchId = matchId;
            return this;
        }

        public Builder transactionId(Long transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder actorId(Long actorId) {
            this.actorId = actorId;
            return this;
        }

        public Builder participantId(Long participantId) {
            this.participantId = participantId;
            return this;
        }

        public Builder side(Integer side) {
            this.side = side;
            return this;
        }

        public Builder priceBetween(Long minPrice, Long maxPrice) {
            this.minPrice = minPrice;
            this.maxPrice = maxPrice;
            return this;
        }

        public Builder quantityBetween(Long minQuantity, Long maxQuantity) {
            this.minQuantity = minQuantity;
            this.maxQuantity = maxQuantity;
            return this;
        }

        public Builder timestampBetween(Long fromTimestamp, Long toTimestamp) {
            this.fromTimestamp = fromTimestamp;
            this.toTimestamp = toTimestamp;
            return this;
        }

        public Builder partitionId(Integer partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public Builder seqnumBetween(Long minSeqnum, Long maxSeqnum) {
            this.minSeqnum = minSeqnum;
            this.maxSeqnum = maxSeqnum;
            return this;
        }

        /** Adds a condition on one of the structure's own fields. */
        public Builder field(FieldCriterion criterion) {
            if (criterion != null) {
                this.fieldCriteria.add(criterion);
            }
            return this;
        }

        /** Parses and adds {@code name=value} against the given structure's schema. */
        public Builder field(MessageSchema schema, String name, String value) {
            return field(FieldCriterion.parse(schema.require(name), value));
        }

        public MessageFilter build() {
            return new MessageFilter(this);
        }

        private static String blankToNull(String value) {
            return value == null || value.isBlank() ? null : value.trim();
        }
    }
}
