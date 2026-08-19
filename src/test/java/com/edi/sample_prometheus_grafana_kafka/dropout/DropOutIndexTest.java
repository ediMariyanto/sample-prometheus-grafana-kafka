package com.edi.sample_prometheus_grafana_kafka.dropout;

import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutIndex;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutIndexBuilder;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.InstrumentSnapshot;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.OrderBookView;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderBook;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.DropOutRowReader;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.ParseStats;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

class DropOutIndexTest {

    private static final long ATIC_ASSET_ID = 3310L;
    private static final long ATIC_RG = 5138L;
    private static final long BBCA_RG = 6728L;
    private static final long COMPOSITE = 78L;

    private static DropOutIndex index;
    private static ParseStats stats;

    @BeforeAll
    static void loadFixture() throws IOException {
        DropOutIndexBuilder builder = new DropOutIndexBuilder();
        try (InputStream in = DropOutRowReaderTest.sampleRows()) {
            stats = new DropOutRowReader().read(in, builder);
        }
        index = builder.build(stats);
    }

    @Test
    void retainsEveryTypeByDefaultSoNothingIsInvisibleToAFilter() {
        assertThat(stats.rows()).isEqualTo(31);
        assertThat(stats.skipped()).isZero();
        assertThat(stats.materialized()).isEqualTo(31);
        assertThat(index.skippedTypes()).isEmpty();
        assertThat(index.retainedCounts().keySet())
                .containsExactlyInAnyOrderElementsOf(java.util.EnumSet.allOf(MessageType.class));
    }

    @Test
    void indexesReferenceData() {
        assertThat(index.assetCount()).isEqualTo(2);
        assertThat(index.orderBookCount()).isEqualTo(6);
        assertThat(index.participantCount()).isEqualTo(1);
        assertThat(index.actorCount()).isEqualTo(1);
        assertThat(index.businessDate()).isEqualTo(1777939200000000000L);
    }

    @Test
    void looksUpAnAssetByTickerAndReturnsAllItsOrderBooks() {
        InstrumentSnapshot snapshot = index.lookup("ATIC").orElseThrow();

        assertThat(snapshot.matchedBy()).isEqualTo(InstrumentSnapshot.MatchedBy.ASSET_NAME);
        assertThat(snapshot.asset().id()).isEqualTo(ATIC_ASSET_ID);
        assertThat(snapshot.asset().isin()).isEqualTo("ID1000134505");
        assertThat(snapshot.orderBooks())
                .extracting(view -> view.orderBook().name())
                .containsExactlyInAnyOrder("ATIC_RG", "ATIC_TN", "ATIC_NG");
    }

    @Test
    void lookupIsCaseInsensitiveAndIgnoresSurroundingSpace() {
        assertThat(index.lookup("  atic  ").orElseThrow().asset().id()).isEqualTo(ATIC_ASSET_ID);
    }

    @Test
    void looksUpByIsin() {
        InstrumentSnapshot snapshot = index.lookup("ID1000134505").orElseThrow();

        assertThat(snapshot.matchedBy()).isEqualTo(InstrumentSnapshot.MatchedBy.ISIN);
        assertThat(snapshot.asset().name()).isEqualTo("ATIC");
    }

    @Test
    void looksUpASingleBookByName() {
        InstrumentSnapshot snapshot = index.lookup("ATIC_RG").orElseThrow();

        assertThat(snapshot.matchedBy()).isEqualTo(InstrumentSnapshot.MatchedBy.ORDER_BOOK_NAME);
        assertThat(snapshot.asset().name()).isEqualTo("ATIC");
        assertThat(snapshot.orderBooks()).hasSize(1);
        assertThat(snapshot.orderBooks().get(0).orderBook().id()).isEqualTo(ATIC_RG);
    }

    @Test
    void looksUpByNumericIdPreferringOrderBooksOverAssets() {
        assertThat(index.lookup("5138").orElseThrow().matchedBy())
                .isEqualTo(InstrumentSnapshot.MatchedBy.ORDER_BOOK_ID);
        // 3310 is an asset id and not an order book id, so it falls through to the asset.
        assertThat(index.lookup("3310").orElseThrow().matchedBy())
                .isEqualTo(InstrumentSnapshot.MatchedBy.ASSET_ID);
    }

    @Test
    void returnsEmptyForAnUnknownKey() {
        assertThat(index.lookup("NOSUCHTICKER")).isEmpty();
        assertThat(index.lookup("")).isEmpty();
        assertThat(index.lookup(null)).isEmpty();
    }

    @Test
    void keepsTheLastStateSeenForEachOrderBook() {
        OrderBookView view = index.orderBookView(ATIC_RG).orElseThrow();

        // The fixture carries two session rows for this book; the later one wins.
        assertThat(view.session().name()).isEqualTo("SOBD");
        assertThat(view.priceLimit().lowerLimit()).isEqualTo(540L);
        assertThat(view.priceLimit().upperLimit()).isEqualTo(790L);
        assertThat(view.referencePrice().referencePrice()).isEqualTo(635L);
        assertThat(view.equilibrium().sessionId()).isEqualTo(25);
        assertThat(view.tradeCount()).isZero();
    }

    @Test
    void leavesStateNullWhenTheFileCarriedNone() {
        OrderBookView view = index.orderBookView(BBCA_RG).orElseThrow();

        assertThat(view.orderBook().name()).isEqualTo("BBCA_RG");
        assertThat(view.topOfBook()).isNotNull();
        assertThat(view.tradeCount()).isEqualTo(2);
        assertThat(view.session()).isNull();
        assertThat(view.statistics()).isNull();
    }

    @Test
    void groupsTradesAndOrdersForRetrieval() {
        assertThat(index.trades(BBCA_RG)).hasSize(2);
        assertThat(index.tradeCount()).isEqualTo(2);
        assertThat(index.order(814520412879740770L)).isPresent();
        assertThat(index.order(1L)).isEmpty();
        assertThat(index.rejects()).hasSize(1);
        assertThat(index.rejects().get(0).errorCode()).isEqualTo(-420131);
    }

    @Test
    void exposesIndexConstituents() {
        assertThat(index.indexMembers(COMPOSITE))
                .hasSize(2)
                .extracting(member -> member.memberOrderBookId())
                .containsExactlyInAnyOrder(7226L, 7208L);
        assertThat(index.indexMembers(999L)).isEmpty();
    }

    @Test
    void resolvesOrderBooksBackToTheirAsset() {
        assertThat(index.orderBooksOfAsset(ATIC_ASSET_ID))
                .extracting(OrderBook::id)
                .containsExactlyInAnyOrder(5138L, 5139L, 5140L);
        assertThat(index.assetByName("ATIC")).isPresent();
        assertThat(index.orderBookByName("atic_rg")).isPresent();
        assertThat(index.participant(3L)).isPresent();
        assertThat(index.actor(2L)).isPresent();
    }
}
