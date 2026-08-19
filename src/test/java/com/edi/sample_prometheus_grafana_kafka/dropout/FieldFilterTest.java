package com.edi.sample_prometheus_grafana_kafka.dropout;

import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutIndex;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutIndexBuilder;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutQueries;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.MessageFilter;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.MessageSchema;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Asset;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderBook;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Trade;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.DropOutRowReader;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.ParseStats;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Filtering each structure by its own fields, not just the fields shared across structures. */
class FieldFilterTest {

    private static DropOutIndex index;
    private static DropOutQueries q;

    @BeforeAll
    static void loadFixture() throws IOException {
        DropOutIndexBuilder builder = new DropOutIndexBuilder();
        ParseStats stats;
        try (InputStream in = DropOutRowReaderTest.sampleRows()) {
            stats = new DropOutRowReader().read(in, builder);
        }
        index = builder.build(stats);
        q = index.queries();
    }

    private static MessageFilter on(MessageType type, String field, String value) {
        return MessageFilter.builder().field(MessageSchema.of(type), field, value).build();
    }

    @Test
    void typedMethodsReturnTheStructuresOwnRecordWithoutCasting() {
        // The whole point: no cast, type-specific accessors reachable directly.
        Trade trade = q.trades(MessageFilter.builder().side(1).build()).get(0);
        assertThat(trade.matchId()).isEqualTo(814520412879716353L);
        assertThat(trade.tradePrice()).isEqualTo(460L);

        OrderBook book = q.orderBooks(on(MessageType.ORDER_BOOK_DIRECTORY, "name", "ATIC_RG")).get(0);
        assertThat(book.lotSize()).isEqualTo(100L);

        Asset asset = q.assets(on(MessageType.ASSET_DIRECTORY, "sectorCode", "I121")).get(0);
        assertThat(asset.name()).isEqualTo("ATIC");
    }

    @Test
    void everyStructureHasATypedMethodAndTheyAgreeWithTheGenericCounts() {
        assertThat(q.orders()).hasSize(1);
        assertThat(q.orderBooks()).hasSize(6);
        assertThat(q.assets()).hasSize(2);
        assertThat(q.participants()).hasSize(1);
        assertThat(q.actors()).hasSize(1);
        assertThat(q.directoryEnds()).hasSize(1);
        assertThat(q.priceLimits()).hasSize(2);
        assertThat(q.indexPrices()).hasSize(1);
        assertThat(q.referencePrices()).hasSize(2);
        assertThat(q.equilibriumPrices()).hasSize(2);
        assertThat(q.orderRejects()).hasSize(1);
        assertThat(q.tradingSessions()).hasSize(2);
        assertThat(q.businessDates()).hasSize(1);
        assertThat(q.transactionBegins()).hasSize(1);
        assertThat(q.transactionEnds()).hasSize(1);
        assertThat(q.trades()).hasSize(2);
        assertThat(q.topOfBooks()).hasSize(1);
        assertThat(q.marketStatistics()).hasSize(1);
        assertThat(q.indexCompositions()).hasSize(2);

        for (MessageType type : MessageType.values()) {
            assertThat(index.query(type, MessageFilter.NONE, 0, 100).matched())
                    .as("%s", type)
                    .isEqualTo(index.retainedCounts().getOrDefault(type, 0));
        }
    }

    @Test
    void filtersTextFieldsThatOnlyOneStructureHas() {
        assertThat(q.assets(on(MessageType.ASSET_DIRECTORY, "assetClassName", "EQUITY"))).hasSize(1);
        assertThat(q.assets(on(MessageType.ASSET_DIRECTORY, "assetClassName", "INDEX"))).hasSize(1);
        assertThat(q.orderBooks(on(MessageType.ORDER_BOOK_DIRECTORY, "priceType", "M"))).hasSize(4);
        assertThat(q.trades(on(MessageType.TRADE, "account", "IDD001111111112"))).hasSize(1);
        assertThat(q.tradingSessions(on(MessageType.TRADING_SESSION, "matchingType", "N"))).hasSize(2);
    }

    @Test
    void textMatchingIgnoresCase() {
        assertThat(q.assets(on(MessageType.ASSET_DIRECTORY, "assetClassName", "equity"))).hasSize(1);
        assertThat(q.orderBooks(on(MessageType.ORDER_BOOK_DIRECTORY, "currency", "idr"))).hasSize(6);
    }

    @Test
    void filtersNumericFieldsIncludingNegatives() {
        assertThat(q.orderRejects(on(MessageType.ORDER_REJECT, "errorCode", "-420131"))).hasSize(1);
        assertThat(q.orderRejects(on(MessageType.ORDER_REJECT, "errorCode", "-1"))).isEmpty();
        assertThat(q.equilibriumPrices(on(MessageType.EQUILIBRIUM_PRICE, "sessionId", "25"))).hasSize(1);
        assertThat(q.indexCompositions(on(MessageType.INDEX_COMPOSITION, "memberOrderBookId", "7226"))).hasSize(1);
    }

    @Test
    void aCommaSeparatedValueMatchesAnyOfThem() {
        assertThat(q.orderBooks(on(MessageType.ORDER_BOOK_DIRECTORY, "marketSegmentId", "5,6"))).hasSize(2);
        assertThat(q.orderBooks(on(MessageType.ORDER_BOOK_DIRECTORY, "marketSegmentId", "5,6,33,71"))).hasSize(4);
        assertThat(q.orderBooks(on(MessageType.ORDER_BOOK_DIRECTORY, "name", "ATIC_RG,BBCA_RG"))).hasSize(2);
    }

    @Test
    void aDoubleDotIsAnInclusiveRange() {
        // lot sizes in the fixture: 1, 100, 100, 100, 1000, 1000
        assertThat(q.orderBooks(on(MessageType.ORDER_BOOK_DIRECTORY, "lotSize", "1..100"))).hasSize(4);
        assertThat(q.orderBooks(on(MessageType.ORDER_BOOK_DIRECTORY, "lotSize", "..99"))).hasSize(1);
        assertThat(q.orderBooks(on(MessageType.ORDER_BOOK_DIRECTORY, "lotSize", "1000.."))).hasSize(2);
        assertThat(q.indexCompositions(on(MessageType.INDEX_COMPOSITION, "weight", "1000000000..")))
                .hasSize(1);
    }

    @Test
    void filtersBooleanFields() {
        assertThat(q.orderBooks(on(MessageType.ORDER_BOOK_DIRECTORY, "tradable", "true"))).hasSize(4);
        assertThat(q.orderBooks(on(MessageType.ORDER_BOOK_DIRECTORY, "tradable", "false"))).hasSize(2);
        assertThat(q.actors(on(MessageType.ACTOR_DIRECTORY, "testActor", "false"))).hasSize(1);
        assertThat(q.participants(on(MessageType.PARTICIPANT_DIRECTORY, "active", "true"))).hasSize(1);
    }

    @Test
    void theWireNameWorksToo() {
        assertThat(q.trades(on(MessageType.TRADE, "subset_seqnum", "1.."))).hasSize(2);
        assertThat(q.trades(on(MessageType.TRADE, "subsetSeqnum", "1.."))).hasSize(2);
    }

    @Test
    void severalFieldConditionsCombineWithAnd() {
        MessageFilter both = MessageFilter.builder()
                .field(MessageSchema.of(MessageType.ORDER_BOOK_DIRECTORY), "tradable", "true")
                .field(MessageSchema.of(MessageType.ORDER_BOOK_DIRECTORY), "lotSize", "100")
                .build();
        assertThat(q.orderBooks(both)).hasSize(3);

        MessageFilter withCrossType = MessageFilter.builder()
                .field(MessageSchema.of(MessageType.ORDER_BOOK_DIRECTORY), "marketSegmentId", "5")
                .name("ATIC_RG")
                .build();
        assertThat(q.orderBooks(withCrossType)).hasSize(1);
    }

    @Test
    void aFieldOfAnotherStructureMatchesNothing() {
        // A condition built against ASSET_DIRECTORY cannot be satisfied by price-limit rows.
        MessageFilter assetCriterion = on(MessageType.ASSET_DIRECTORY, "sectorCode", "I121");

        assertThat(index.query(MessageType.PRICE_LIMIT, assetCriterion, 0, 100).matched()).isZero();
        assertThat(index.query(MessageType.ASSET_DIRECTORY, assetCriterion, 0, 100).matched()).isEqualTo(1);
    }

    @Test
    void aMalformedValueIsRejectedRatherThanIgnored() {
        MessageSchema orderBook = MessageSchema.of(MessageType.ORDER_BOOK_DIRECTORY);

        assertThatThrownBy(() -> MessageFilter.builder().field(orderBook, "lotSize", "abc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is numeric, but got 'abc'");

        assertThatThrownBy(() -> MessageFilter.builder().field(orderBook, "tradable", "yes"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is a flag; use true or false");

        assertThatThrownBy(() -> MessageFilter.builder().field(orderBook, "lotSize", "1000..1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inverted");

        assertThatThrownBy(() -> MessageFilter.builder().field(orderBook, "nosuchfield", "1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown field");
    }

    @Test
    void theTypedMethodRejectsAMismatchedRecordClass() {
        assertThatThrownBy(() -> index.filter(MessageType.TRADE, OrderBook.class, MessageFilter.NONE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TRADE is Trade, not OrderBook");
    }
}
