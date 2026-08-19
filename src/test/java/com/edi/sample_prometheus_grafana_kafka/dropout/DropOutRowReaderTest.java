package com.edi.sample_prometheus_grafana_kafka.dropout;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketMessage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketValues;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderBook;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderMessage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Trade;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.DropOutRowReader;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.ParseStats;
import com.edi.sample_prometheus_grafana_kafka.dropout.parser.RowHandler;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DropOutRowReaderTest {

    private final DropOutRowReader reader = new DropOutRowReader();

    static InputStream sampleRows() {
        InputStream in = DropOutRowReaderTest.class.getResourceAsStream("/dropout/sample-rows.jsonl");
        assertThat(in).as("test fixture /dropout/sample-rows.jsonl").isNotNull();
        return in;
    }

    private static InputStream bytes(String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }

    /** Collects every message the reader emits. */
    private static final class Collecting implements RowHandler {
        final List<MarketMessage> messages = new ArrayList<>();
        final Set<MessageType> skip;

        Collecting() {
            this(Set.of());
        }

        Collecting(Set<MessageType> skip) {
            this.skip = skip;
        }

        @Override
        public boolean wants(MessageType type) {
            return !skip.contains(type);
        }

        @Override
        public void onMessage(MessageType type, MarketMessage message) {
            messages.add(message);
        }

        @SuppressWarnings("unchecked")
        <T extends MarketMessage> List<T> of(Class<T> messageClass) {
            List<T> found = new ArrayList<>();
            for (MarketMessage message : messages) {
                if (messageClass.isInstance(message)) {
                    found.add((T) message);
                }
            }
            return found;
        }
    }

    @Test
    void readsEveryRowAndCountsThemByType() throws IOException {
        Collecting handler = new Collecting();

        ParseStats stats;
        try (InputStream in = sampleRows()) {
            stats = reader.read(in, handler);
        }

        assertThat(stats.rows()).isEqualTo(31);
        assertThat(stats.materialized()).isEqualTo(31);
        assertThat(stats.skipped()).isZero();
        assertThat(stats.unknown()).isZero();
        assertThat(stats.malformed()).isZero();
        assertThat(handler.messages).hasSize(31);

        // Every modelled message type appears in the fixture, so nothing goes untested.
        assertThat(stats.countsByType().keySet()).containsExactlyInAnyOrderElementsOf(EnumSet.allOf(MessageType.class));
        assertThat(stats.count(MessageType.ORDER_BOOK_DIRECTORY)).isEqualTo(6);
        assertThat(stats.count(MessageType.TRADE)).isEqualTo(2);
    }

    @Test
    void bindsNestedAndSnakeCaseFields() throws IOException {
        Collecting handler = new Collecting();
        try (InputStream in = sampleRows()) {
            reader.read(in, handler);
        }

        OrderBook regular = handler.of(OrderBook.class).stream()
                .filter(book -> book.id() == 5138L)
                .findFirst()
                .orElseThrow();

        assertThat(regular.name()).isEqualTo("ATIC_RG");
        assertThat(regular.assetId()).isEqualTo(3310L);
        assertThat(regular.lotSize()).isEqualTo(100L);
        assertThat(regular.marketSegmentId()).isEqualTo(5);
        assertThat(regular.currency()).isEqualTo("IDR");
        // subset_seqnum / tcp_seqnum are snake_case on the wire.
        assertThat(regular.subsetSeqnum()).isPositive();
        assertThat(regular.tcpSeqnum()).isPositive();
        // PriceTick is a capitalised nested array.
        assertThat(regular.priceTicks()).isNotEmpty();
        assertThat(regular.priceTicks().get(0).stepSize()).isPositive();
        assertThat(regular.combinationLegs()).isEmpty();
        assertThat(regular.messageType()).isEqualTo(MessageType.ORDER_BOOK_DIRECTORY);
    }

    @Test
    void preservesFullLongPrecisionAndTheAbsentSentinel() throws IOException {
        Collecting handler = new Collecting();
        try (InputStream in = sampleRows()) {
            reader.read(in, handler);
        }

        OrderMessage order = handler.of(OrderMessage.class).get(0);
        assertThat(order.orderId()).isEqualTo(814520412879740770L);
        assertThat(order.orderBookId()).isEqualTo(6728L);
        assertThat(order.price()).isEqualTo(550L);
        assertThat(order.account()).isEqualTo("IDD001111111111");
        // The feed writes Long.MIN_VALUE where a field carries no value.
        assertThat(order.displayQuantity()).isEqualTo(MarketValues.ABSENT);
        assertThat(MarketValues.isPresent(order.displayQuantity())).isFalse();
        assertThat(MarketValues.isPresent(order.price())).isTrue();

        List<Trade> trades = handler.of(Trade.class);
        assertThat(trades).hasSize(2);
        assertThat(trades).allSatisfy(trade -> assertThat(trade.matchId()).isEqualTo(814520412879716353L));
        // Both sides of the same match are reported.
        assertThat(trades.stream().map(Trade::side)).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void skippedTypesAreCountedButNeverBound() throws IOException {
        Collecting handler = new Collecting(EnumSet.of(MessageType.TRANSACTION_BEGIN, MessageType.TRANSACTION_END));

        ParseStats stats;
        try (InputStream in = sampleRows()) {
            stats = reader.read(in, handler);
        }

        assertThat(stats.rows()).isEqualTo(31);
        assertThat(stats.skipped()).isEqualTo(2);
        assertThat(stats.materialized()).isEqualTo(29);
        assertThat(handler.messages).hasSize(29);
        // Counting still works for types that were never turned into objects.
        assertThat(stats.count(MessageType.TRANSACTION_BEGIN)).isEqualTo(1);
        assertThat(stats.count(MessageType.TRANSACTION_END)).isEqualTo(1);
    }

    @Test
    void toleratesCrlfBlankLinesAndAMissingFinalNewline() throws IOException {
        String text = "{\"messageId\":17,\"businessDate\":1777939200000000000,\"offset\":1}\r\n"
                + "\r\n"
                + "   \r\n"
                + "{\"messageId\":18,\"transactionId\":7,\"offset\":2}";
        Collecting handler = new Collecting();

        ParseStats stats = reader.read(bytes(text), handler);

        assertThat(stats.rows()).isEqualTo(2);
        assertThat(handler.messages).hasSize(2);
    }

    @Test
    void growsTheBufferForRowsLongerThanIt() throws IOException {
        // A 1KiB buffer cannot hold a single fixture row, forcing the grow-and-compact path.
        DropOutRowReader tiny = new DropOutRowReader(DropOutRowReader.defaultMapper(), 1024);
        Collecting handler = new Collecting();

        ParseStats stats;
        try (InputStream in = sampleRows()) {
            stats = tiny.read(in, handler);
        }

        assertThat(stats.rows()).isEqualTo(31);
        assertThat(handler.messages).hasSize(31);
    }

    @Test
    void reportsUnknownMessageIdsWithoutFailing() throws IOException {
        List<Integer> unknown = new ArrayList<>();
        RowHandler handler = new RowHandler() {
            @Override
            public void onMessage(MessageType type, MarketMessage message) {
            }

            @Override
            public void onUnknownMessageId(int messageId, long rowNumber) {
                unknown.add(messageId);
            }
        };

        ParseStats stats = reader.read(bytes("{\"messageId\":9999,\"offset\":1}\n"), handler);

        assertThat(unknown).containsExactly(9999);
        assertThat(stats.unknown()).isEqualTo(1);
        assertThat(stats.materialized()).isZero();
    }

    @Test
    void rejectsARowWithNoMessageId() {
        RowHandler handler = (type, message) -> {
        };

        assertThatThrownBy(() -> reader.read(bytes("{\"offset\":1}\n"), handler))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no messageId");
    }
}
