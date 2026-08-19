package com.edi.sample_prometheus_grafana_kafka.dropout.index;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.Actor;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketMessage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Asset;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.EquilibriumPrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.IndexComposition;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.IndexPrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketStatistics;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.EnumMap;
import java.util.Optional;
import java.util.Set;

/**
 * Immutable in-memory view of one drop-copy file, built by {@link DropOutIndexBuilder}.
 *
 * <p>Reference data (assets, order books, participants, actors) is kept in full. For per-book state
 * only the <em>last</em> message of each kind is retained, which is what "the state at end of file"
 * means for a sequential feed. Every lookup is a hash lookup - no scanning.
 *
 * <p>Separately, every row is retained grouped by {@link MessageType}, which is what {@link #query}
 * filters over. The two views answer different questions: the lookup maps answer "what is the state
 * of this instrument now", the retained rows answer "show me the rows of this message type that
 * match these criteria".
 */
public final class DropOutIndex {

    private final Map<Long, Asset> assetsById;
    private final Map<String, Asset> assetsByName;
    private final Map<String, Asset> assetsByIsin;

    private final Map<Long, OrderBook> orderBooksById;
    private final Map<String, OrderBook> orderBooksByName;
    private final Map<Long, List<OrderBook>> orderBooksByAssetId;

    private final Map<Long, Participant> participantsById;
    private final Map<Long, Actor> actorsById;

    private final Map<Long, TradingSession> sessionsByOrderBookId;
    private final Map<Long, PriceLimit> priceLimitsByOrderBookId;
    private final Map<Long, ReferencePrice> referencePricesByOrderBookId;
    private final Map<Long, EquilibriumPrice> equilibriaByOrderBookId;
    private final Map<Long, TopOfBook> topOfBooksByOrderBookId;
    private final Map<Long, MarketStatistics> statisticsByOrderBookId;
    private final Map<Long, IndexPrice> indexPricesByOrderBookId;

    private final Map<Long, List<IndexComposition>> membersByIndexOrderBookId;
    private final Map<Long, OrderMessage> ordersById;
    private final Map<Long, List<Trade>> tradesByOrderBookId;
    private final List<OrderReject> rejects;

    private final Map<MessageType, List<MarketMessage>> messagesByType;
    private final Set<MessageType> skippedTypes;
    private final Long businessDate;
    private final ParseStats stats;

    DropOutIndex(Map<Long, Asset> assetsById,
                 Map<String, Asset> assetsByName,
                 Map<String, Asset> assetsByIsin,
                 Map<Long, OrderBook> orderBooksById,
                 Map<String, OrderBook> orderBooksByName,
                 Map<Long, List<OrderBook>> orderBooksByAssetId,
                 Map<Long, Participant> participantsById,
                 Map<Long, Actor> actorsById,
                 Map<Long, TradingSession> sessionsByOrderBookId,
                 Map<Long, PriceLimit> priceLimitsByOrderBookId,
                 Map<Long, ReferencePrice> referencePricesByOrderBookId,
                 Map<Long, EquilibriumPrice> equilibriaByOrderBookId,
                 Map<Long, TopOfBook> topOfBooksByOrderBookId,
                 Map<Long, MarketStatistics> statisticsByOrderBookId,
                 Map<Long, IndexPrice> indexPricesByOrderBookId,
                 Map<Long, List<IndexComposition>> membersByIndexOrderBookId,
                 Map<Long, OrderMessage> ordersById,
                 Map<Long, List<Trade>> tradesByOrderBookId,
                 List<OrderReject> rejects,
                 Map<MessageType, List<MarketMessage>> messagesByType,
                 Set<MessageType> skippedTypes,
                 Long businessDate,
                 ParseStats stats) {
        this.assetsById = assetsById;
        this.assetsByName = assetsByName;
        this.assetsByIsin = assetsByIsin;
        this.orderBooksById = orderBooksById;
        this.orderBooksByName = orderBooksByName;
        this.orderBooksByAssetId = orderBooksByAssetId;
        this.participantsById = participantsById;
        this.actorsById = actorsById;
        this.sessionsByOrderBookId = sessionsByOrderBookId;
        this.priceLimitsByOrderBookId = priceLimitsByOrderBookId;
        this.referencePricesByOrderBookId = referencePricesByOrderBookId;
        this.equilibriaByOrderBookId = equilibriaByOrderBookId;
        this.topOfBooksByOrderBookId = topOfBooksByOrderBookId;
        this.statisticsByOrderBookId = statisticsByOrderBookId;
        this.indexPricesByOrderBookId = indexPricesByOrderBookId;
        this.membersByIndexOrderBookId = membersByIndexOrderBookId;
        this.ordersById = ordersById;
        this.tradesByOrderBookId = tradesByOrderBookId;
        this.rejects = rejects;
        this.messagesByType = messagesByType;
        this.skippedTypes = skippedTypes;
        this.businessDate = businessDate;
        this.stats = stats;
    }

    /**
     * The general lookup: resolves a ticker ({@code ATIC}), an ISIN ({@code ID1000134505}), an order
     * book name ({@code ATIC_RG}) or a numeric order book / asset id, and returns the matching
     * instrument joined to its latest market state.
     *
     * <p>Matching is case-insensitive and tries, in order: order book name, asset name, ISIN, then -
     * if the query is all digits - order book id followed by asset id. Names are checked before ids
     * because a ticker is what callers usually have.
     */
    public Optional<InstrumentSnapshot> lookup(String query) {
        if (query == null) {
            return Optional.empty();
        }
        String key = query.trim().toUpperCase(java.util.Locale.ROOT);
        if (key.isEmpty()) {
            return Optional.empty();
        }

        OrderBook book = orderBooksByName.get(key);
        if (book != null) {
            return Optional.of(snapshotOfBook(query, InstrumentSnapshot.MatchedBy.ORDER_BOOK_NAME, book));
        }

        Asset asset = assetsByName.get(key);
        if (asset != null) {
            return Optional.of(snapshotOfAsset(query, InstrumentSnapshot.MatchedBy.ASSET_NAME, asset));
        }

        asset = assetsByIsin.get(key);
        if (asset != null) {
            return Optional.of(snapshotOfAsset(query, InstrumentSnapshot.MatchedBy.ISIN, asset));
        }

        Long id = parseId(key);
        if (id != null) {
            book = orderBooksById.get(id);
            if (book != null) {
                return Optional.of(snapshotOfBook(query, InstrumentSnapshot.MatchedBy.ORDER_BOOK_ID, book));
            }
            asset = assetsById.get(id);
            if (asset != null) {
                return Optional.of(snapshotOfAsset(query, InstrumentSnapshot.MatchedBy.ASSET_ID, asset));
            }
        }
        return Optional.empty();
    }

    private static Long parseId(String key) {
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c < '0' || c > '9') {
                return null;
            }
        }
        try {
            return Long.valueOf(key);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private InstrumentSnapshot snapshotOfAsset(String query, InstrumentSnapshot.MatchedBy matchedBy, Asset asset) {
        List<OrderBook> books = orderBooksByAssetId.getOrDefault(asset.id(), List.of());
        List<OrderBookView> views = new ArrayList<>(books.size());
        for (OrderBook book : books) {
            views.add(viewOf(book));
        }
        return new InstrumentSnapshot(query, matchedBy, asset, List.copyOf(views));
    }

    private InstrumentSnapshot snapshotOfBook(String query, InstrumentSnapshot.MatchedBy matchedBy, OrderBook book) {
        return new InstrumentSnapshot(query, matchedBy, assetsById.get(book.assetId()), List.of(viewOf(book)));
    }

    /** Joins a single order book to its latest state. */
    public OrderBookView viewOf(OrderBook book) {
        long id = book.id();
        return new OrderBookView(
                book,
                sessionsByOrderBookId.get(id),
                priceLimitsByOrderBookId.get(id),
                referencePricesByOrderBookId.get(id),
                equilibriaByOrderBookId.get(id),
                topOfBooksByOrderBookId.get(id),
                statisticsByOrderBookId.get(id),
                indexPricesByOrderBookId.get(id),
                tradesByOrderBookId.getOrDefault(id, List.of()).size());
    }

    public Optional<OrderBookView> orderBookView(long orderBookId) {
        return orderBook(orderBookId).map(this::viewOf);
    }


    /**
     * Filters the retained rows of one message type. The returned page holds the messages in their
     * own shape - a {@code TRADE} page carries trade fields, a {@code PRICE_LIMIT} page carries
     * price-limit fields - because the discriminating {@code messageId} already chose the record.
     *
     * <p>The scan is linear over that type's rows only, never over the whole file: asking for
     * trades never touches the 25k price-limit rows.
     *
     * @param page zero-based page index
     * @param size page size; the page reports {@code matched} alongside {@code returned} so a
     *             truncated result is visible
     */
    public MessagePage query(MessageType type, MessageFilter filter, int page, int size) {
        List<MarketMessage> rows = messagesByType.get(type);
        if (rows == null || rows.isEmpty()) {
            return MessagePage.empty(type, page, size);
        }
        MessageFilter criteria = filter == null ? MessageFilter.NONE : filter;
        long from = (long) page * size; // long, so a large page index cannot overflow into a negative

        List<MarketMessage> content = new ArrayList<>(Math.min(size, 64));
        int matched = 0;
        for (MarketMessage row : rows) {
            if (!criteria.test(row)) {
                continue;
            }
            if (matched >= from && content.size() < size) {
                content.add(row);
            }
            matched++;
        }
        return new MessagePage(type, type.id(), rows.size(), matched, page, size, content.size(),
                Collections.unmodifiableList(content));
    }

    /** All retained rows of a type, in file order. */
    public List<MarketMessage> messages(MessageType type) {
        return messagesByType.getOrDefault(type, List.of());
    }

    /**
     * Filters one structure and returns its own record type, so callers reach type-specific fields
     * without casting. Unpaged - use {@link #query} when the result set may be large.
     */
    public <T extends MarketMessage> List<T> filter(MessageType type, Class<T> as, MessageFilter filter) {
        if (!as.equals(type.messageClass())) {
            throw new IllegalArgumentException(
                    type.name() + " is " + type.messageClass().getSimpleName() + ", not " + as.getSimpleName());
        }
        MessageFilter criteria = filter == null ? MessageFilter.NONE : filter;
        List<T> matches = new ArrayList<>();
        for (MarketMessage row : messages(type)) {
            if (criteria.test(row)) {
                matches.add(as.cast(row));
            }
        }
        return Collections.unmodifiableList(matches);
    }

    /** Typed, per-structure filter methods - one for each {@code messageId}. */
    public DropOutQueries queries() {
        return new DropOutQueries(this);
    }

    /** The field layout of a structure: what can be filtered on it, and under which names. */
    public MessageSchema schema(MessageType type) {
        return MessageSchema.of(type);
    }

    /** How many rows of each type the index is holding. */
    public Map<MessageType, Integer> retainedCounts() {
        Map<MessageType, Integer> counts = new EnumMap<>(MessageType.class);
        messagesByType.forEach((type, rows) -> counts.put(type, rows.size()));
        return counts;
    }

    /** Types deliberately not retained at load time, and therefore not filterable. */
    public Set<MessageType> skippedTypes() {
        return skippedTypes;
    }

    /** Order book ids a symbol resolves to: a ticker gives all its books, a book name gives one. */
    public Set<Long> resolveOrderBookIds(String symbol) {
        return lookup(symbol)
                .map(snapshot -> snapshot.orderBooks().stream()
                        .map(view -> view.orderBook().id())
                        .collect(java.util.stream.Collectors.toUnmodifiableSet()))
                .orElse(Set.of());
    }

    public Optional<Asset> asset(long id) {
        return Optional.ofNullable(assetsById.get(id));
    }

    public Optional<Asset> assetByName(String name) {
        return Optional.ofNullable(assetsByName.get(normalise(name)));
    }

    public Optional<Asset> assetByIsin(String isin) {
        return Optional.ofNullable(assetsByIsin.get(normalise(isin)));
    }

    public Optional<OrderBook> orderBook(long id) {
        return Optional.ofNullable(orderBooksById.get(id));
    }

    public Optional<OrderBook> orderBookByName(String name) {
        return Optional.ofNullable(orderBooksByName.get(normalise(name)));
    }

    public List<OrderBook> orderBooksOfAsset(long assetId) {
        return orderBooksByAssetId.getOrDefault(assetId, List.of());
    }

    public Optional<Participant> participant(long id) {
        return Optional.ofNullable(participantsById.get(id));
    }

    public Optional<Actor> actor(long id) {
        return Optional.ofNullable(actorsById.get(id));
    }

    public Optional<OrderMessage> order(long orderId) {
        return Optional.ofNullable(ordersById.get(orderId));
    }

    public List<Trade> trades(long orderBookId) {
        return tradesByOrderBookId.getOrDefault(orderBookId, List.of());
    }

    /** Constituents of an index book, e.g. the members of {@code COMPOSITE}. */
    public List<IndexComposition> indexMembers(long indexOrderBookId) {
        return membersByIndexOrderBookId.getOrDefault(indexOrderBookId, List.of());
    }

    public List<OrderReject> rejects() {
        return rejects;
    }

    /** Trading day of the file as epoch nanoseconds, or {@code null} when the file carried none. */
    public Long businessDate() {
        return businessDate;
    }

    public ParseStats stats() {
        return stats;
    }

    public int assetCount() {
        return assetsById.size();
    }

    public int orderBookCount() {
        return orderBooksById.size();
    }

    public int participantCount() {
        return participantsById.size();
    }

    public int actorCount() {
        return actorsById.size();
    }

    public int orderCount() {
        return ordersById.size();
    }

    public int tradeCount() {
        int total = 0;
        for (List<Trade> trades : tradesByOrderBookId.values()) {
            total += trades.size();
        }
        return total;
    }

    public Map<Long, OrderBook> orderBooksById() {
        return Collections.unmodifiableMap(orderBooksById);
    }

    public Map<Long, Asset> assetsById() {
        return Collections.unmodifiableMap(assetsById);
    }

    private static String normalise(String value) {
        return value == null ? "" : value.trim().toUpperCase(java.util.Locale.ROOT);
    }
}
