package com.edi.sample_prometheus_grafana_kafka.dropout;

import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutIndex;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutIndexBuilder;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.MessageFilter;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.MessagePage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Asset;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderBook;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.PriceLimit;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Trade;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.DropOutRowReader;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.ParseStats;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class MessageFilterTest {

    private static final long ATIC_RG = 5138L;
    private static final long BBCA_RG = 6728L;

    private static DropOutIndex index;

    @BeforeAll
    static void loadFixture() throws IOException {
        DropOutIndexBuilder builder = new DropOutIndexBuilder();
        ParseStats stats;
        try (InputStream in = DropOutRowReaderTest.sampleRows()) {
            stats = new DropOutRowReader().read(in, builder);
        }
        index = builder.build(stats);
    }

    private static MessagePage query(MessageType type, MessageFilter filter) {
        return index.query(type, filter, 0, 100);
    }

    @Test
    void anEmptyFilterReturnsEveryRowOfTheType() {
        MessagePage page = query(MessageType.TRADE, MessageFilter.NONE);

        assertThat(page.type()).isEqualTo(MessageType.TRADE);
        assertThat(page.messageId()).isEqualTo(20);
        assertThat(page.retained()).isEqualTo(2);
        assertThat(page.matched()).isEqualTo(2);
        assertThat(page.returned()).isEqualTo(2);
    }

    @Test
    void contentKeepsEachTypeItsOwnShape() {
        assertThat(query(MessageType.TRADE, MessageFilter.NONE).content())
                .allMatch(Trade.class::isInstance);
        assertThat(query(MessageType.PRICE_LIMIT, MessageFilter.NONE).content())
                .allMatch(PriceLimit.class::isInstance);
        assertThat(query(MessageType.ORDER_BOOK_DIRECTORY, MessageFilter.NONE).content())
                .allMatch(OrderBook.class::isInstance);

        // The discriminating field really does select a different field set.
        Trade trade = (Trade) query(MessageType.TRADE, MessageFilter.NONE).content().get(0);
        assertThat(trade.matchId()).isEqualTo(814520412879716353L);
        PriceLimit limit = (PriceLimit) query(MessageType.PRICE_LIMIT, MessageFilter.NONE).content().get(0);
        assertThat(limit.upperLimit()).isEqualTo(790L);
    }

    @Test
    void filtersBySide() {
        assertThat(query(MessageType.TRADE, MessageFilter.builder().side(1).build()).matched()).isEqualTo(1);
        assertThat(query(MessageType.TRADE, MessageFilter.builder().side(2).build()).matched()).isEqualTo(1);
        assertThat(query(MessageType.TRADE, MessageFilter.builder().side(9).build()).matched()).isZero();
    }

    @Test
    void filtersByOrderBook() {
        MessageFilter aticRg = MessageFilter.builder().orderBookIds(Set.of(ATIC_RG)).build();

        assertThat(query(MessageType.PRICE_LIMIT, aticRg).matched()).isEqualTo(2);
        assertThat(query(MessageType.TRADING_SESSION, aticRg).matched()).isEqualTo(2);
        // Trades in the fixture belong to a different book.
        assertThat(query(MessageType.TRADE, aticRg).matched()).isZero();
        assertThat(query(MessageType.TRADE, MessageFilter.builder().orderBookIds(Set.of(BBCA_RG)).build()).matched())
                .isEqualTo(2);
    }

    @Test
    void aSymbolExpandsToEveryOrderBookOfTheInstrument() {
        Set<Long> books = index.resolveOrderBookIds("ATIC");
        assertThat(books).containsExactlyInAnyOrder(5138L, 5139L, 5140L);

        MessageFilter filter = MessageFilter.builder().orderBookIds(books).build();
        assertThat(query(MessageType.ORDER_BOOK_DIRECTORY, filter).matched()).isEqualTo(3);

        // A single book name resolves to just that book.
        assertThat(index.resolveOrderBookIds("ATIC_RG")).containsExactly(ATIC_RG);
        assertThat(index.resolveOrderBookIds("NOSUCHTICKER")).isEmpty();
    }

    @Test
    void aCriterionTheTypeDoesNotHaveMatchesNothingRatherThanEverything() {
        // A price limit has no order, no side and no match, so these must exclude it outright.
        MessageFilter byOrder = MessageFilter.builder().orderId(814520412879740770L).build();
        assertThat(query(MessageType.PRICE_LIMIT, byOrder).matched()).isZero();
        assertThat(query(MessageType.ORDER, byOrder).matched()).isEqualTo(1);

        assertThat(query(MessageType.TRADING_SESSION, MessageFilter.builder().side(1).build()).matched()).isZero();
        assertThat(query(MessageType.PRICE_LIMIT, MessageFilter.builder().isin("ID1000134505").build()).matched())
                .isZero();
    }

    @Test
    void filtersDirectoriesByName() {
        // Two session rows for the same book, distinguished only by name.
        assertThat(query(MessageType.TRADING_SESSION, MessageFilter.builder().name("SOBD").build()).matched())
                .isEqualTo(1);
        assertThat(query(MessageType.TRADING_SESSION, MessageFilter.builder().name("endofday").build()).matched())
                .as("name matching is case-insensitive")
                .isEqualTo(1);
        assertThat(query(MessageType.ORDER_BOOK_DIRECTORY, MessageFilter.builder().name("ATIC_TN").build()).matched())
                .isEqualTo(1);
    }

    @Test
    void filtersAssetsByIsin() {
        MessagePage page = query(MessageType.ASSET_DIRECTORY, MessageFilter.builder().isin("ID1000134505").build());

        assertThat(page.matched()).isEqualTo(1);
        assertThat(((Asset) page.content().get(0)).name()).isEqualTo("ATIC");
    }

    @Test
    void filtersByPriceAndQuantityRange() {
        assertThat(query(MessageType.TRADE, MessageFilter.builder().priceBetween(400L, 500L).build()).matched())
                .isEqualTo(2);
        assertThat(query(MessageType.TRADE, MessageFilter.builder().priceBetween(600L, null).build()).matched())
                .isZero();
        assertThat(query(MessageType.TRADE, MessageFilter.builder().quantityBetween(100000L, null).build()).matched())
                .isEqualTo(2);
        // The order sits at 550, above the trades.
        assertThat(query(MessageType.ORDER, MessageFilter.builder().priceBetween(500L, 600L).build()).matched())
                .isEqualTo(1);
    }

    @Test
    void filtersTransactionEnvelopesById() {
        MessageFilter filter = MessageFilter.builder().transactionId(69293L).build();

        assertThat(query(MessageType.TRANSACTION_BEGIN, filter).matched()).isEqualTo(1);
        assertThat(query(MessageType.TRANSACTION_END, filter).matched()).isEqualTo(1);
        assertThat(query(MessageType.TRANSACTION_END, MessageFilter.builder().transactionId(1L).build()).matched())
                .isZero();
    }

    @Test
    void filtersByTimestampWindow() {
        long oneDay = 86_400_000_000_000L;
        long businessDate = index.businessDate();
        MessageFilter aroundTheBusinessDate = MessageFilter.builder()
                .timestampBetween(businessDate - oneDay, businessDate + oneDay)
                .build();

        assertThat(query(MessageType.TRADING_SESSION, aroundTheBusinessDate).matched()).isEqualTo(2);
        assertThat(query(MessageType.TRADE, aroundTheBusinessDate).matched()).isEqualTo(2);

        // A window ending before any row matches nothing.
        assertThat(query(MessageType.TRADE,
                MessageFilter.builder().timestampBetween(null, businessDate - oneDay).build()).matched()).isZero();
    }

    @Test
    void businessDateIsTheTradingDayLabelNotALowerBoundOnTimestamps() {
        // businessDate is UTC midnight of the trading day, but the exchange runs at UTC+7, so the
        // pre-open and reference-data rows are stamped on the *previous* UTC calendar day. Filtering
        // from businessDate therefore silently drops them - use a real instant, not the business date.
        long businessDate = index.businessDate();

        assertThat(query(MessageType.TRADING_SESSION, MessageFilter.builder()
                .timestampBetween(businessDate, null).build()).matched())
                .as("session rows are stamped before UTC midnight of the business date")
                .isZero();
        assertThat(query(MessageType.TRADE, MessageFilter.builder()
                .timestampBetween(businessDate, null).build()).matched())
                .as("trades happen during the session, after UTC midnight")
                .isEqualTo(2);
    }

    @Test
    void aTimeWindowExcludesTypesThatCarryNoClock() {
        long oneDay = 86_400_000_000_000L;
        long businessDate = index.businessDate();
        MessageFilter anyTime = MessageFilter.builder()
                .timestampBetween(businessDate - oneDay, businessDate + oneDay)
                .build();

        // TRANSACTION_BEGIN has no timestamp field at all, so no window can include it.
        assertThat(query(MessageType.TRANSACTION_BEGIN, anyTime).matched()).isZero();
        assertThat(query(MessageType.TRANSACTION_BEGIN, MessageFilter.NONE).matched()).isEqualTo(1);
    }

    @Test
    void reportsMatchedSeparatelyFromReturnedSoTruncationIsVisible() {
        MessagePage first = index.query(MessageType.ORDER_BOOK_DIRECTORY, MessageFilter.NONE, 0, 2);

        assertThat(first.retained()).isEqualTo(6);
        assertThat(first.matched()).isEqualTo(6);
        assertThat(first.returned()).isEqualTo(2);
        assertThat(first.page()).isZero();

        MessagePage last = index.query(MessageType.ORDER_BOOK_DIRECTORY, MessageFilter.NONE, 2, 2);
        assertThat(last.returned()).isEqualTo(2);

        MessagePage past = index.query(MessageType.ORDER_BOOK_DIRECTORY, MessageFilter.NONE, 99, 2);
        assertThat(past.matched()).isEqualTo(6);
        assertThat(past.returned()).isZero();
    }

    @Test
    void criteriaCombineWithAnd() {
        MessageFilter both = MessageFilter.builder()
                .orderBookIds(Set.of(BBCA_RG))
                .side(1)
                .build();
        assertThat(query(MessageType.TRADE, both).matched()).isEqualTo(1);

        MessageFilter contradictory = MessageFilter.builder()
                .orderBookIds(Set.of(ATIC_RG))
                .side(1)
                .build();
        assertThat(query(MessageType.TRADE, contradictory).matched()).isZero();
    }

    @Test
    void skippingATypeIsExplicitAndReported() throws IOException {
        DropOutIndexBuilder builder = new DropOutIndexBuilder(DropOutIndexBuilder.ENVELOPE_TYPES);
        ParseStats stats;
        try (InputStream in = DropOutRowReaderTest.sampleRows()) {
            stats = new DropOutRowReader().read(in, builder);
        }
        DropOutIndex lean = builder.build(stats);

        assertThat(stats.skipped()).isEqualTo(2);
        assertThat(lean.skippedTypes())
                .containsExactlyInAnyOrder(MessageType.TRANSACTION_BEGIN, MessageType.TRANSACTION_END);
        // Skipped types hold no rows, and the index says so rather than pretending they were empty.
        assertThat(lean.query(MessageType.TRANSACTION_END, MessageFilter.NONE, 0, 100).retained()).isZero();
        assertThat(lean.retainedCounts()).doesNotContainKey(MessageType.TRANSACTION_END);
        // Everything else is unaffected.
        assertThat(lean.query(MessageType.TRADE, MessageFilter.NONE, 0, 100).matched()).isEqualTo(2);
    }
}
