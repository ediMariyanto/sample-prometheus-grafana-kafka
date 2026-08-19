package com.edi.sample_prometheus_grafana_kafka.dropout.index;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.Actor;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Asset;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.BusinessDate;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.EquilibriumPrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.IndexComposition;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.IndexPrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketMessage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketStatistics;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderBook;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderMessage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderReject;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Participant;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.PriceLimit;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.ReferencePrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.TopOfBook;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Trade;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.TradingSession;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.ParseStats;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.RowHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Consumes the row stream from the reader and accumulates a {@link DropOutIndex}.
 *
 * <p>Two things accumulate at once: the lookup maps, which keep only the latest state per order
 * book, and the full per-type row lists that back {@link DropOutIndex#query}. Every type is
 * retained by default so nothing is invisible to a filter; pass a skip set to trade completeness
 * for parse time and memory, and the set is carried into the index so the omission is reported
 * rather than silent.
 *
 * <p>Not thread-safe - one builder per parse.
 */
public final class DropOutIndexBuilder implements RowHandler {

    /**
     * Envelope-only, high-volume types. They carry no state a lookup can answer, so callers that
     * only need lookups can skip them and roughly halve parse time - but they are retained by
     * default, because skipping them would make them invisible to {@link DropOutIndex#query}.
     */
    public static final Set<MessageType> ENVELOPE_TYPES =
            Collections.unmodifiableSet(EnumSet.of(MessageType.TRANSACTION_BEGIN, MessageType.TRANSACTION_END));

    private final Set<MessageType> skipped;

    private final Map<Long, Asset> assetsById = new HashMap<>(2048);
    private final Map<String, Asset> assetsByName = new HashMap<>(2048);
    private final Map<String, Asset> assetsByIsin = new HashMap<>(2048);

    private final Map<Long, OrderBook> orderBooksById = new HashMap<>(8192);
    private final Map<String, OrderBook> orderBooksByName = new HashMap<>(8192);
    private final Map<Long, List<OrderBook>> orderBooksByAssetId = new HashMap<>(2048);

    private final Map<Long, Participant> participantsById = new HashMap<>(256);
    private final Map<Long, Actor> actorsById = new HashMap<>(2048);

    private final Map<Long, TradingSession> sessionsByOrderBookId = new HashMap<>(8192);
    private final Map<Long, PriceLimit> priceLimitsByOrderBookId = new HashMap<>(8192);
    private final Map<Long, ReferencePrice> referencePricesByOrderBookId = new HashMap<>(8192);
    private final Map<Long, EquilibriumPrice> equilibriaByOrderBookId = new HashMap<>(8192);
    private final Map<Long, TopOfBook> topOfBooksByOrderBookId = new HashMap<>(1024);
    private final Map<Long, MarketStatistics> statisticsByOrderBookId = new HashMap<>(1024);
    private final Map<Long, IndexPrice> indexPricesByOrderBookId = new HashMap<>(256);

    private final Map<Long, List<IndexComposition>> membersByIndexOrderBookId = new HashMap<>(64);
    private final Map<Long, OrderMessage> ordersById = new LinkedHashMap<>();
    private final Map<Long, List<Trade>> tradesByOrderBookId = new HashMap<>();
    private final List<OrderReject> rejects = new ArrayList<>();

    private final Map<MessageType, List<MarketMessage>> messagesByType = new EnumMap<>(MessageType.class);

    private Long businessDate;

    /** Retains every message type, so all of them can be filtered afterwards. */
    public DropOutIndexBuilder() {
        this(Set.of());
    }

    public DropOutIndexBuilder(Set<MessageType> skipped) {
        this.skipped = skipped.isEmpty() ? Set.of() : EnumSet.copyOf(skipped);
    }

    @Override
    public boolean wants(MessageType type) {
        return !skipped.contains(type);
    }

    @Override
    public void onMessage(MessageType type, MarketMessage message) {
        // Retained in arrival order so a filtered page reads in the same order as the file.
        messagesByType.computeIfAbsent(type, key -> new ArrayList<>()).add(message);
        switch (type) {
            case ASSET_DIRECTORY -> {
                Asset asset = (Asset) message;
                assetsById.put(asset.id(), asset);
                assetsByName.put(normalise(asset.name()), asset);
                String isin = normalise(asset.isin());
                if (!isin.isEmpty()) {
                    assetsByIsin.put(isin, asset);
                }
            }
            case ORDER_BOOK_DIRECTORY -> {
                OrderBook book = (OrderBook) message;
                orderBooksById.put(book.id(), book);
                orderBooksByName.put(normalise(book.name()), book);
                orderBooksByAssetId.computeIfAbsent(book.assetId(), key -> new ArrayList<>(4)).add(book);
            }
            case PARTICIPANT_DIRECTORY -> {
                Participant participant = (Participant) message;
                participantsById.put(participant.id(), participant);
            }
            case ACTOR_DIRECTORY -> {
                Actor actor = (Actor) message;
                actorsById.put(actor.id(), actor);
            }
            case TRADING_SESSION -> {
                TradingSession session = (TradingSession) message;
                sessionsByOrderBookId.put(session.orderBookId(), session);
            }
            case PRICE_LIMIT -> {
                PriceLimit limit = (PriceLimit) message;
                priceLimitsByOrderBookId.put(limit.orderBookId(), limit);
            }
            case REFERENCE_PRICE -> {
                ReferencePrice price = (ReferencePrice) message;
                referencePricesByOrderBookId.put(price.orderBookId(), price);
            }
            case EQUILIBRIUM_PRICE -> {
                EquilibriumPrice equilibrium = (EquilibriumPrice) message;
                equilibriaByOrderBookId.put(equilibrium.orderBookId(), equilibrium);
            }
            case TOP_OF_BOOK -> {
                TopOfBook top = (TopOfBook) message;
                topOfBooksByOrderBookId.put(top.orderBookId(), top);
            }
            case MARKET_STATISTICS -> {
                MarketStatistics statistics = (MarketStatistics) message;
                statisticsByOrderBookId.put(statistics.orderBookId(), statistics);
            }
            case INDEX_PRICE -> {
                IndexPrice price = (IndexPrice) message;
                indexPricesByOrderBookId.put(price.orderBookId(), price);
            }
            case INDEX_COMPOSITION -> {
                IndexComposition member = (IndexComposition) message;
                membersByIndexOrderBookId.computeIfAbsent(member.indexOrderBookId(), key -> new ArrayList<>()).add(member);
            }
            case ORDER -> {
                OrderMessage order = (OrderMessage) message;
                ordersById.put(order.orderId(), order); // last state of the order wins
            }
            case TRADE -> {
                Trade trade = (Trade) message;
                tradesByOrderBookId.computeIfAbsent(trade.orderBookId(), key -> new ArrayList<>()).add(trade);
            }
            case ORDER_REJECT -> rejects.add((OrderReject) message);
            case BUSINESS_DATE -> businessDate = ((BusinessDate) message).businessDate();
            default -> {
                // DIRECTORY_END and the transaction envelopes carry no indexable state.
            }
        }
    }

    public DropOutIndex build(ParseStats stats) {
        return new DropOutIndex(
                Map.copyOf(assetsById),
                Map.copyOf(assetsByName),
                Map.copyOf(assetsByIsin),
                Map.copyOf(orderBooksById),
                Map.copyOf(orderBooksByName),
                freezeLists(orderBooksByAssetId),
                Map.copyOf(participantsById),
                Map.copyOf(actorsById),
                Map.copyOf(sessionsByOrderBookId),
                Map.copyOf(priceLimitsByOrderBookId),
                Map.copyOf(referencePricesByOrderBookId),
                Map.copyOf(equilibriaByOrderBookId),
                Map.copyOf(topOfBooksByOrderBookId),
                Map.copyOf(statisticsByOrderBookId),
                Map.copyOf(indexPricesByOrderBookId),
                freezeLists(membersByIndexOrderBookId),
                Map.copyOf(ordersById),
                freezeLists(tradesByOrderBookId),
                List.copyOf(rejects),
                freezeByType(),
                skipped,
                businessDate,
                stats);
    }

    private Map<MessageType, List<MarketMessage>> freezeByType() {
        Map<MessageType, List<MarketMessage>> frozen = new EnumMap<>(MessageType.class);
        messagesByType.forEach((type, messages) -> frozen.put(type, Collections.unmodifiableList(messages)));
        return Collections.unmodifiableMap(frozen);
    }

    private static <T> Map<Long, List<T>> freezeLists(Map<Long, List<T>> source) {
        Map<Long, List<T>> frozen = new HashMap<>(Math.max(16, source.size() * 2));
        source.forEach((key, values) -> frozen.put(key, List.copyOf(values)));
        return Collections.unmodifiableMap(frozen);
    }

    private static String normalise(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }
}
