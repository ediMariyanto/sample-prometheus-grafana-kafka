package com.edi.sample_prometheus_grafana_kafka.dropout;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.io.IOException;
import java.io.InputStream;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end through the real parser and index - the fixture is uploaded exactly as a client would
 * upload a drop-copy file.
 */
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DropOutControllerTest {

    @Autowired
    private MockMvc mockMvc;

    private static MockMultipartFile fixture() throws IOException {
        try (InputStream in = DropOutRowReaderTest.sampleRows()) {
            return new MockMultipartFile("file", "sample-rows.jsonl",
                    MediaType.APPLICATION_OCTET_STREAM_VALUE, in.readAllBytes());
        }
    }

    @Test
    @Order(1)
    void queryingBeforeAnyUploadIsAConflict() throws Exception {
        mockMvc.perform(get("/v1/dropout/summary"))
                .andExpect(status().isConflict());
        mockMvc.perform(get("/v1/dropout/order-books/5138"))
                .andExpect(status().isConflict());
    }

    @Test
    @Order(2)
    void uploadReturnsAParseAndIndexSummary() throws Exception {
        mockMvc.perform(multipart("/v1/dropout/upload").file(fixture()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.filename").value("sample-rows.jsonl"))
                .andExpect(jsonPath("$.rows").value(31))
                .andExpect(jsonPath("$.skippedRows").value(0))
                .andExpect(jsonPath("$.malformedRows").value(0))
                .andExpect(jsonPath("$.businessDate").value("2026-05-05T00:00:00Z"))
                .andExpect(jsonPath("$.index.orderBooks").value(6))
                .andExpect(jsonPath("$.index.assets").value(2))
                .andExpect(jsonPath("$.index.trades").value(2))
                .andExpect(jsonPath("$.rowsByType.TRADE").value(2));
    }

    @Test
    @Order(3)
    void lookupResolvesATickerToItsBooksAndState() throws Exception {
        mockMvc.perform(get("/v1/dropout/lookup/{query}", "ATIC"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matchedBy").value("ASSET_NAME"))
                .andExpect(jsonPath("$.asset.name").value("ATIC"))
                .andExpect(jsonPath("$.asset.isin").value("ID1000134505"))
                .andExpect(jsonPath("$.orderBooks.length()").value(3));

        mockMvc.perform(get("/v1/dropout/lookup/{query}", "ATIC_RG"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matchedBy").value("ORDER_BOOK_NAME"))
                .andExpect(jsonPath("$.orderBooks.length()").value(1))
                .andExpect(jsonPath("$.orderBooks[0].session.name").value("SOBD"))
                .andExpect(jsonPath("$.orderBooks[0].priceLimit.upperLimit").value(790));
    }

    @Test
    @Order(4)
    void unknownInstrumentIsNotFound() throws Exception {
        mockMvc.perform(get("/v1/dropout/lookup/{query}", "NOSUCHTICKER"))
                .andExpect(status().isNotFound());
        mockMvc.perform(get("/v1/dropout/orders/{id}", 42))
                .andExpect(status().isNotFound());
    }

    @Test
    @Order(5)
    void exposesTradesOrdersAndIndexMembers() throws Exception {
        mockMvc.perform(get("/v1/dropout/order-books/{id}/trades", 6728))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].matchId").value(814520412879716353L));

        mockMvc.perform(get("/v1/dropout/orders/{id}", 814520412879740770L))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.account").value("IDD001111111111"));

        mockMvc.perform(get("/v1/dropout/indices/{id}/members", 78))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));

        mockMvc.perform(get("/v1/dropout/rejects"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].errorCode").value(-420131));
    }

    @Test
    @Order(6)
    void listsFilterableTypesWithTheirRetainedCounts() throws Exception {
        mockMvc.perform(get("/v1/dropout/messages"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.retainedByType.TRADE").value(2))
                .andExpect(jsonPath("$.retainedByType.ORDER_BOOK_DIRECTORY").value(6))
                .andExpect(jsonPath("$.skippedTypes.length()").value(0))
                .andExpect(jsonPath("$.messageIds.TRADE").value(20));
    }

    @Test
    @Order(7)
    void filtersByTypeAndReturnsThatTypesOwnFields() throws Exception {
        mockMvc.perform(get("/v1/dropout/messages/{type}", "TRADE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.type").value("TRADE"))
                .andExpect(jsonPath("$.messageId").value(20))
                .andExpect(jsonPath("$.matched").value(2))
                .andExpect(jsonPath("$.content[0].matchId").value(814520412879716353L))
                .andExpect(jsonPath("$.content[0].tradePrice").value(460));

        // A price-limit page carries price-limit fields, not trade fields.
        mockMvc.perform(get("/v1/dropout/messages/{type}", "PRICE_LIMIT"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].upperLimit").value(790))
                .andExpect(jsonPath("$.content[0].matchId").doesNotExist());
    }

    @Test
    @Order(8)
    void theTypeMayBeGivenAsTheRawMessageId() throws Exception {
        mockMvc.perform(get("/v1/dropout/messages/{type}", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.type").value("TRADE"))
                .andExpect(jsonPath("$.matched").value(2));
    }

    @Test
    @Order(9)
    void appliesQueryParametersAsFilters() throws Exception {
        mockMvc.perform(get("/v1/dropout/messages/{type}", "TRADE").param("side", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(1))
                .andExpect(jsonPath("$.content[0].side").value(1));

        // A ticker expands to all three of its order books.
        mockMvc.perform(get("/v1/dropout/messages/{type}", "PRICE_LIMIT").param("symbol", "ATIC"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(2));

        mockMvc.perform(get("/v1/dropout/messages/{type}", "TRADE").param("symbol", "ATIC"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(0));

        mockMvc.perform(get("/v1/dropout/messages/{type}", "TRADE")
                        .param("minPrice", "400").param("maxPrice", "500"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(2));
    }

    @Test
    @Order(10)
    void pagesAndReportsTruncation() throws Exception {
        mockMvc.perform(get("/v1/dropout/messages/{type}", "ORDER_BOOK_DIRECTORY")
                        .param("size", "2").param("page", "0"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.retained").value(6))
                .andExpect(jsonPath("$.matched").value(6))
                .andExpect(jsonPath("$.returned").value(2))
                .andExpect(jsonPath("$.content.length()").value(2));
    }

    @Test
    @Order(11)
    void acceptsAnIsoInstantForTheTimeWindow() throws Exception {
        // The fixture's trades are stamped 2026-05-05T07:13Z.
        mockMvc.perform(get("/v1/dropout/messages/{type}", "TRADE")
                        .param("from", "2026-05-05T00:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(2));

        mockMvc.perform(get("/v1/dropout/messages/{type}", "TRADE")
                        .param("from", "2026-05-05T00:00:00Z")
                        .param("to", "2026-05-05T07:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(0));

        // Raw epoch nanoseconds work too, since that is what the feed carries.
        mockMvc.perform(get("/v1/dropout/messages/{type}", "TRADE")
                        .param("from", "1777965193583793879"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(2));
    }

    @Test
    @Order(12)
    void rejectsAnUnknownTypeOrAnUnparseableTime() throws Exception {
        mockMvc.perform(get("/v1/dropout/messages/{type}", "NOSUCHTYPE"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("unknown message type: NOSUCHTYPE"));

        mockMvc.perform(get("/v1/dropout/messages/{type}", "TRADE").param("from", "yesterday"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @Order(13)
    void describesEachStructuresFields() throws Exception {
        mockMvc.perform(get("/v1/dropout/messages/{type}/fields", "TRADE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.type").value("TRADE"))
                .andExpect(jsonPath("$.messageId").value(20))
                .andExpect(jsonPath("$.structure").value("Trade"))
                .andExpect(jsonPath("$.fieldCount").value(30))
                .andExpect(jsonPath("$.fields[?(@.name=='matchId')].kind").value("NUMBER"))
                .andExpect(jsonPath("$.fields[?(@.name=='account')].kind").value("TEXT"))
                .andExpect(jsonPath("$.fields[?(@.name=='subsetSeqnum')].wireName").value("subset_seqnum"));

        // A different messageId really is a different field set.
        mockMvc.perform(get("/v1/dropout/messages/{type}/fields", "7"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.type").value("PRICE_LIMIT"))
                .andExpect(jsonPath("$.fieldCount").value(12))
                .andExpect(jsonPath("$.fields[?(@.name=='upperLimit')]").exists())
                .andExpect(jsonPath("$.fields[?(@.name=='matchId')]").doesNotExist());

        // Nested arrays are listed but flagged unfilterable rather than quietly omitted.
        mockMvc.perform(get("/v1/dropout/messages/{type}/fields", "ORDER_BOOK_DIRECTORY"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.fields[?(@.name=='priceTicks')].filterable").value(false))
                .andExpect(jsonPath("$.fields[?(@.name=='priceTicks')].wireName").value("PriceTick"));
    }

    @Test
    @Order(14)
    void filtersByAStructuresOwnFieldsFromTheQueryString() throws Exception {
        mockMvc.perform(get("/v1/dropout/messages/{type}", "ASSET_DIRECTORY").param("sectorCode", "I121"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(1))
                .andExpect(jsonPath("$.content[0].name").value("ATIC"));

        mockMvc.perform(get("/v1/dropout/messages/{type}", "ORDER_REJECT").param("errorCode", "-420131"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(1));

        mockMvc.perform(get("/v1/dropout/messages/{type}", "ORDER_BOOK_DIRECTORY").param("tradable", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(4));

        // Comma list, range, and a repeated parameter all mean "any of these".
        mockMvc.perform(get("/v1/dropout/messages/{type}", "ORDER_BOOK_DIRECTORY")
                        .param("marketSegmentId", "5,6"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(2));

        mockMvc.perform(get("/v1/dropout/messages/{type}", "ORDER_BOOK_DIRECTORY").param("lotSize", "1..100"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(4));

        mockMvc.perform(get("/v1/dropout/messages/{type}", "ORDER_BOOK_DIRECTORY")
                        .param("marketSegmentId", "5").param("marketSegmentId", "33"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(2));

        // Field conditions AND with the cross-type ones.
        mockMvc.perform(get("/v1/dropout/messages/{type}", "ORDER_BOOK_DIRECTORY")
                        .param("tradable", "true").param("symbol", "ATIC"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(3));
    }

    @Test
    @Order(15)
    void anUnknownFieldIsRejectedAndSaysWhatIsAvailable() throws Exception {
        mockMvc.perform(get("/v1/dropout/messages/{type}", "PRICE_LIMIT").param("sectorCode", "I121"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value(org.hamcrest.Matchers.containsString(
                        "unknown field 'sectorCode' for PRICE_LIMIT")))
                .andExpect(jsonPath("$.error").value(org.hamcrest.Matchers.containsString("upperLimit")));

        mockMvc.perform(get("/v1/dropout/messages/{type}", "ORDER_BOOK_DIRECTORY").param("lotSize", "abc"))
                .andExpect(status().isBadRequest());

        mockMvc.perform(get("/v1/dropout/messages/{type}", "ORDER_BOOK_DIRECTORY").param("PriceTick", "1"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value(org.hamcrest.Matchers.containsString("nested structure")));
    }

    @Test
    @Order(16)
    void anEmptyUploadIsRejected() throws Exception {
        MockMultipartFile empty = new MockMultipartFile("file", "empty.jsonl",
                MediaType.APPLICATION_OCTET_STREAM_VALUE, new byte[0]);

        mockMvc.perform(multipart("/v1/dropout/upload").file(empty))
                .andExpect(status().isBadRequest());
    }

    @Test
    @Order(17)
    void anUnknownMessageIdInSkipIsRejectedAtUpload() throws Exception {
        mockMvc.perform(multipart("/v1/dropout/upload").file(fixture()).param("skip", "999"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("unknown message type: 999"));

        mockMvc.perform(multipart("/v1/dropout/upload").file(fixture()).param("skip", "NOSUCHTYPE"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @Order(18)
    void skippingEveryMessageTypeIsRejected() throws Exception {
        String allTypes = Arrays.stream(MessageType.values())
                .map(MessageType::name)
                .collect(Collectors.joining(","));

        mockMvc.perform(multipart("/v1/dropout/upload").file(fixture()).param("skip", allTypes))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value(org.hamcrest.Matchers.containsString(
                        "cannot skip all 19 message types")));
    }

    @Test
    @Order(19)
    void queryingASkippedTypeSaysSoInsteadOfReturningAnEmptyPage() throws Exception {
        mockMvc.perform(multipart("/v1/dropout/upload").file(fixture())
                        .param("skip", "TRANSACTION_BEGIN,19"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.skippedRows").value(2))
                .andExpect(jsonPath("$.skippedTypes.length()").value(2));

        // Not an empty page - an explicit conflict naming how to fix it.
        mockMvc.perform(get("/v1/dropout/messages/{type}", "TRANSACTION_END"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value(org.hamcrest.Matchers.containsString(
                        "TRANSACTION_END (messageId 19) was skipped")))
                .andExpect(jsonPath("$.error").value(org.hamcrest.Matchers.containsString(
                        "re-upload without skip=TRANSACTION_END")));

        // The schema is independent of what was loaded, so it still answers.
        mockMvc.perform(get("/v1/dropout/messages/{type}/fields", "TRANSACTION_END"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.fieldCount").value(9));

        // Types that were not skipped are unaffected.
        mockMvc.perform(get("/v1/dropout/messages/{type}", "TRADE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(2));
    }

    @Test
    @Order(20)
    void aTypeWithNoRowsIsAnEmptyPageNotAConflict() throws Exception {
        // Two rows, so PRICE_LIMIT is genuinely absent rather than skipped.
        String tiny = "{\"messageId\":17,\"businessDate\":1777939200000000000,\"offset\":1}\n"
                + "{\"messageId\":20,\"matchId\":7,\"orderBookId\":9,\"tradePrice\":100,"
                + "\"quantity\":5,\"side\":1,\"offset\":2}\n";
        MockMultipartFile file = new MockMultipartFile("file", "tiny.jsonl",
                MediaType.APPLICATION_OCTET_STREAM_VALUE, tiny.getBytes(StandardCharsets.UTF_8));

        mockMvc.perform(multipart("/v1/dropout/upload").file(file))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.rows").value(2))
                .andExpect(jsonPath("$.skippedTypes.length()").value(0));

        mockMvc.perform(get("/v1/dropout/messages/{type}", "PRICE_LIMIT"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.retained").value(0))
                .andExpect(jsonPath("$.matched").value(0))
                .andExpect(jsonPath("$.content.length()").value(0));

        mockMvc.perform(get("/v1/dropout/messages/{type}", "TRADE"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.matched").value(1))
                .andExpect(jsonPath("$.content[0].tradePrice").value(100));
    }
}
