package com.edi.sample_prometheus_grafana_kafka.dropout;

import com.edi.sample_prometheus_grafana_kafka.dropout.index.FieldSpec;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.MessageSchema;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The schema is what {@code messageId} discriminates, derived from each structure's record. */
class MessageSchemaTest {

    @Test
    void everyMessageTypeHasASchemaCoveringAllItsFields() {
        int total = 0;
        for (MessageType type : MessageType.values()) {
            MessageSchema schema = MessageSchema.of(type);
            assertThat(schema).as("schema for %s", type).isNotNull();
            assertThat(schema.type()).isEqualTo(type);
            assertThat(schema.fields())
                    .as("%s declares fields", type)
                    .hasSize(type.messageClass().getRecordComponents().length)
                    .isNotEmpty();
            total += schema.fields().size();
        }
        // 19 structures, 316 fields between them.
        assertThat(total).isEqualTo(316);
    }

    @Test
    void everyStructureExposesTheSharedEnvelope() {
        for (MessageType type : MessageType.values()) {
            MessageSchema schema = MessageSchema.of(type);
            assertThat(schema.field("offset")).as("%s.offset", type).isPresent();
            assertThat(schema.field("partitionId")).as("%s.partitionId", type).isPresent();
            assertThat(schema.field("messageGroup")).as("%s.messageGroup", type).isPresent();
        }
    }

    @Test
    void fieldsResolveByJavaNameAndByWireNameCaseInsensitively() {
        MessageSchema schema = MessageSchema.of(MessageType.TRADE);

        FieldSpec byJava = schema.field("subsetSeqnum").orElseThrow();
        FieldSpec byWire = schema.field("subset_seqnum").orElseThrow();
        FieldSpec byCase = schema.field("SUBSET_SEQNUM").orElseThrow();

        assertThat(byJava).isSameAs(byWire).isSameAs(byCase);
        assertThat(byJava.name()).isEqualTo("subsetSeqnum");
        assertThat(byJava.wireName()).isEqualTo("subset_seqnum");
    }

    @Test
    void unaliasedFieldsReportTheSameNameForBoth() {
        FieldSpec spec = MessageSchema.of(MessageType.TRADE).field("tradePrice").orElseThrow();

        assertThat(spec.name()).isEqualTo("tradePrice");
        assertThat(spec.wireName()).isEqualTo("tradePrice");
    }

    @Test
    void kindsFollowTheRecordComponentTypes() {
        MessageSchema orderBook = MessageSchema.of(MessageType.ORDER_BOOK_DIRECTORY);

        assertThat(orderBook.field("lotSize").orElseThrow().kind()).isEqualTo(FieldSpec.Kind.NUMBER);
        assertThat(orderBook.field("marketSegmentId").orElseThrow().kind()).isEqualTo(FieldSpec.Kind.NUMBER);
        assertThat(orderBook.field("currency").orElseThrow().kind()).isEqualTo(FieldSpec.Kind.TEXT);
        assertThat(orderBook.field("tradable").orElseThrow().kind()).isEqualTo(FieldSpec.Kind.BOOLEAN);
    }

    @Test
    void nestedStructuresAreListedButNotFilterable() {
        MessageSchema orderBook = MessageSchema.of(MessageType.ORDER_BOOK_DIRECTORY);

        FieldSpec ticks = orderBook.field("priceTicks").orElseThrow();
        assertThat(ticks.wireName()).isEqualTo("PriceTick");
        assertThat(ticks.kind()).isEqualTo(FieldSpec.Kind.UNSUPPORTED);
        assertThat(ticks.filterable()).isFalse();
        assertThat(orderBook.filterableNames()).doesNotContain("priceTicks", "combinationLegs", "repoOrderbook");

        assertThatThrownBy(() -> orderBook.require("PriceTick"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nested structure");
    }

    @Test
    void anUnknownFieldNamesWhatIsAvailable() {
        assertThatThrownBy(() -> MessageSchema.of(MessageType.PRICE_LIMIT).require("sectorCode"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown field 'sectorCode' for PRICE_LIMIT")
                .hasMessageContaining("upperLimit");
    }

    @Test
    void structuresReallyDoDifferByMessageId() {
        // The point of the discriminator: same envelope, different payload.
        assertThat(MessageSchema.of(MessageType.TRADE).filterableNames()).contains("matchId", "tradePrice", "account");
        assertThat(MessageSchema.of(MessageType.PRICE_LIMIT).filterableNames())
                .contains("lowerLimit", "upperLimit")
                .doesNotContain("matchId", "tradePrice");
        assertThat(MessageSchema.of(MessageType.ASSET_DIRECTORY).filterableNames())
                .contains("isin", "sectorCode", "assetClassName")
                .doesNotContain("orderBookId");
    }
}
