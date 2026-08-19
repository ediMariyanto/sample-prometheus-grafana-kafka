package com.edi.sample_prometheus_grafana_kafka.dropout.index;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.Actor;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Asset;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.BusinessDate;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.DirectoryEnd;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.EquilibriumPrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.IndexComposition;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.IndexPrice;
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
import com.edi.sample_prometheus_grafana_kafka.dropout.model.TransactionBegin;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.TransactionEnd;

import java.util.List;

/**
 * One typed filter method per message structure.
 *
 * <p>{@link DropOutIndex#query} is generic and hands back {@code MarketMessage}, which callers must
 * then cast. These methods bind the structure that the {@code messageId} already determined, so the
 * result is the concrete record type and its own fields are reachable without a cast:
 *
 * <pre>{@code
 * List<Trade> fills = index.queries().trades(
 *         MessageFilter.builder().orderBookIds(Set.of(6728L)).side(1).build());
 * long price = fills.get(0).tradePrice();   // no cast, no Optional
 * }</pre>
 *
 * <p>Each method has a no-argument form returning every retained row of that structure.
 */
public final class DropOutQueries {

    private final DropOutIndex index;

    DropOutQueries(DropOutIndex index) {
        this.index = index;
    }

    /** messageId 1 - order state. */
    public List<OrderMessage> orders(MessageFilter filter) {
        return index.filter(MessageType.ORDER, OrderMessage.class, filter);
    }

    public List<OrderMessage> orders() {
        return orders(MessageFilter.NONE);
    }

    /** messageId 2 - order book directory. */
    public List<OrderBook> orderBooks(MessageFilter filter) {
        return index.filter(MessageType.ORDER_BOOK_DIRECTORY, OrderBook.class, filter);
    }

    public List<OrderBook> orderBooks() {
        return orderBooks(MessageFilter.NONE);
    }

    /** messageId 3 - asset directory. */
    public List<Asset> assets(MessageFilter filter) {
        return index.filter(MessageType.ASSET_DIRECTORY, Asset.class, filter);
    }

    public List<Asset> assets() {
        return assets(MessageFilter.NONE);
    }

    /** messageId 4 - participant directory. */
    public List<Participant> participants(MessageFilter filter) {
        return index.filter(MessageType.PARTICIPANT_DIRECTORY, Participant.class, filter);
    }

    public List<Participant> participants() {
        return participants(MessageFilter.NONE);
    }

    /** messageId 5 - actor directory. */
    public List<Actor> actors(MessageFilter filter) {
        return index.filter(MessageType.ACTOR_DIRECTORY, Actor.class, filter);
    }

    public List<Actor> actors() {
        return actors(MessageFilter.NONE);
    }

    /** messageId 6 - end-of-directory marker. */
    public List<DirectoryEnd> directoryEnds(MessageFilter filter) {
        return index.filter(MessageType.DIRECTORY_END, DirectoryEnd.class, filter);
    }

    public List<DirectoryEnd> directoryEnds() {
        return directoryEnds(MessageFilter.NONE);
    }

    /** messageId 7 - auto-rejection band. */
    public List<PriceLimit> priceLimits(MessageFilter filter) {
        return index.filter(MessageType.PRICE_LIMIT, PriceLimit.class, filter);
    }

    public List<PriceLimit> priceLimits() {
        return priceLimits(MessageFilter.NONE);
    }

    /** messageId 9 - index level. */
    public List<IndexPrice> indexPrices(MessageFilter filter) {
        return index.filter(MessageType.INDEX_PRICE, IndexPrice.class, filter);
    }

    public List<IndexPrice> indexPrices() {
        return indexPrices(MessageFilter.NONE);
    }

    /** messageId 10 - reference price. */
    public List<ReferencePrice> referencePrices(MessageFilter filter) {
        return index.filter(MessageType.REFERENCE_PRICE, ReferencePrice.class, filter);
    }

    public List<ReferencePrice> referencePrices() {
        return referencePrices(MessageFilter.NONE);
    }

    /** messageId 11 - indicative auction state. */
    public List<EquilibriumPrice> equilibriumPrices(MessageFilter filter) {
        return index.filter(MessageType.EQUILIBRIUM_PRICE, EquilibriumPrice.class, filter);
    }

    public List<EquilibriumPrice> equilibriumPrices() {
        return equilibriumPrices(MessageFilter.NONE);
    }

    /** messageId 14 - rejected order. */
    public List<OrderReject> orderRejects(MessageFilter filter) {
        return index.filter(MessageType.ORDER_REJECT, OrderReject.class, filter);
    }

    public List<OrderReject> orderRejects() {
        return orderRejects(MessageFilter.NONE);
    }

    /** messageId 15 - trading session change. */
    public List<TradingSession> tradingSessions(MessageFilter filter) {
        return index.filter(MessageType.TRADING_SESSION, TradingSession.class, filter);
    }

    public List<TradingSession> tradingSessions() {
        return tradingSessions(MessageFilter.NONE);
    }

    /** messageId 17 - business date. */
    public List<BusinessDate> businessDates(MessageFilter filter) {
        return index.filter(MessageType.BUSINESS_DATE, BusinessDate.class, filter);
    }

    public List<BusinessDate> businessDates() {
        return businessDates(MessageFilter.NONE);
    }

    /** messageId 18 - transaction open. */
    public List<TransactionBegin> transactionBegins(MessageFilter filter) {
        return index.filter(MessageType.TRANSACTION_BEGIN, TransactionBegin.class, filter);
    }

    public List<TransactionBegin> transactionBegins() {
        return transactionBegins(MessageFilter.NONE);
    }

    /** messageId 19 - transaction close. */
    public List<TransactionEnd> transactionEnds(MessageFilter filter) {
        return index.filter(MessageType.TRANSACTION_END, TransactionEnd.class, filter);
    }

    public List<TransactionEnd> transactionEnds() {
        return transactionEnds(MessageFilter.NONE);
    }

    /** messageId 20 - execution. */
    public List<Trade> trades(MessageFilter filter) {
        return index.filter(MessageType.TRADE, Trade.class, filter);
    }

    public List<Trade> trades() {
        return trades(MessageFilter.NONE);
    }

    /** messageId 22 - best bid / best offer. */
    public List<TopOfBook> topOfBooks(MessageFilter filter) {
        return index.filter(MessageType.TOP_OF_BOOK, TopOfBook.class, filter);
    }

    public List<TopOfBook> topOfBooks() {
        return topOfBooks(MessageFilter.NONE);
    }

    /** messageId 26 - daily statistics. */
    public List<MarketStatistics> marketStatistics(MessageFilter filter) {
        return index.filter(MessageType.MARKET_STATISTICS, MarketStatistics.class, filter);
    }

    public List<MarketStatistics> marketStatistics() {
        return marketStatistics(MessageFilter.NONE);
    }

    /** messageId 101 - index constituent. */
    public List<IndexComposition> indexCompositions(MessageFilter filter) {
        return index.filter(MessageType.INDEX_COMPOSITION, IndexComposition.class, filter);
    }

    public List<IndexComposition> indexCompositions() {
        return indexCompositions(MessageFilter.NONE);
    }
}
