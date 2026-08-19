package com.edi.sample_prometheus_grafana_kafka.dropout.parser;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketMessage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Reads a newline-delimited JSON drop-copy file one row at a time.
 *
 * <p>The file is a stream of heterogeneous messages discriminated by a {@code messageId} field, so
 * a naive reader has to build a tree per row just to find out what the row is. This reader avoids
 * that:
 *
 * <ol>
 *   <li>Rows are split at the byte level out of a large buffer. No {@code String} is created per
 *       row, so the UTF-8 decode happens once, inside Jackson, instead of twice.</li>
 *   <li>The {@code messageId} is found by scanning the raw bytes for the key, which costs a few
 *       hundred nanoseconds and no allocation.</li>
 *   <li>The row is then bound straight from the byte range by a per-type {@link ObjectReader}, so
 *       there is no intermediate {@code JsonNode} or {@code Map}.</li>
 *   <li>Types the handler does not want are never bound at all - only counted. On a typical file
 *       roughly half the rows are transaction begin/end envelopes, so skipping them nearly halves
 *       the work.</li>
 * </ol>
 *
 * <p>Instances are immutable and safe to share; each {@link #read} call uses its own buffer.
 */
public final class DropOutRowReader {

    private static final byte[] MESSAGE_ID_KEY = "\"messageId\":".getBytes(StandardCharsets.US_ASCII);
    private static final int DEFAULT_BUFFER_SIZE = 1 << 20;
    private static final int SNIPPET_LIMIT = 512;

    private static final byte WANT_UNDECIDED = 0;
    private static final byte WANT_YES = 1;
    private static final byte WANT_NO = 2;

    private final ObjectReader[] readersByMessageId = new ObjectReader[MessageType.MAX_ID + 1];
    private final int bufferSize;

    public DropOutRowReader() {
        this(defaultMapper(), DEFAULT_BUFFER_SIZE);
    }

    public DropOutRowReader(ObjectMapper mapper, int bufferSize) {
        if (bufferSize < 1024) {
            throw new IllegalArgumentException("bufferSize must be at least 1024, was " + bufferSize);
        }
        this.bufferSize = bufferSize;
        for (MessageType type : MessageType.values()) {
            readersByMessageId[type.id()] = mapper.readerFor(type.messageClass());
        }
    }

    /**
     * A mapper tolerant of schema drift in both directions: fields the records do not model are
     * ignored, and fields a row omits leave their primitive component at zero rather than failing
     * the whole row. Jackson 3 fails on the latter by default, which would make one short row abort
     * an entire file. Note this is distinct from {@link
     * com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketValues#ABSENT}: the feed states
     * "no value" explicitly with {@code Long.MIN_VALUE}, so zero here means "field was not sent".
     */
    public static ObjectMapper defaultMapper() {
        return JsonMapper.builder()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                .build();
    }

    /**
     * Streams every row of {@code in} to {@code handler}. The stream is read incrementally, so a
     * multi-gigabyte file needs only {@code bufferSize} bytes of buffer plus whatever the handler
     * decides to retain. The caller owns the stream and closes it.
     */
    public ParseStats read(InputStream in, RowHandler handler) throws IOException {
        ParseStats stats = new ParseStats();
        byte[] wanted = new byte[MessageType.MAX_ID + 1];
        byte[] buffer = new byte[bufferSize];

        int limit = 0;      // bytes currently held in the buffer
        int scanFrom = 0;   // first byte not yet examined for a newline
        int rowStart = 0;   // start of the row being accumulated
        boolean endOfStream = false;

        long startedAt = System.nanoTime();
        while (true) {
            int newline = indexOfNewline(buffer, scanFrom, limit);
            if (newline < 0) {
                scanFrom = limit;
                if (endOfStream) {
                    handleRow(buffer, rowStart, limit, handler, wanted, stats);
                    break;
                }
                if (rowStart > 0) {
                    System.arraycopy(buffer, rowStart, buffer, 0, limit - rowStart);
                    limit -= rowStart;
                    scanFrom -= rowStart;
                    rowStart = 0;
                }
                if (limit == buffer.length) {
                    buffer = Arrays.copyOf(buffer, buffer.length << 1);
                }
                int read = in.read(buffer, limit, buffer.length - limit);
                if (read < 0) {
                    endOfStream = true;
                } else {
                    limit += read;
                    stats.addBytes(read);
                }
                continue;
            }
            handleRow(buffer, rowStart, newline, handler, wanted, stats);
            rowStart = newline + 1;
            scanFrom = rowStart;
        }
        stats.finish(System.nanoTime() - startedAt);
        return stats;
    }

    private void handleRow(byte[] buffer, int from, int to, RowHandler handler, byte[] wanted, ParseStats stats) {
        while (to > from && isSkippableWhitespace(buffer[to - 1])) {
            to--;
        }
        while (from < to && isSkippableWhitespace(buffer[from])) {
            from++;
        }
        if (to - from >= 3 && (buffer[from] & 0xFF) == 0xEF
                && (buffer[from + 1] & 0xFF) == 0xBB && (buffer[from + 2] & 0xFF) == 0xBF) {
            from += 3;
        }
        if (from >= to) {
            return; // blank line
        }

        int messageId = readMessageId(buffer, from, to);
        stats.recordRow(messageId);

        MessageType type = messageId < 0 ? null : MessageType.byId(messageId);
        if (type == null) {
            if (messageId < 0) {
                stats.recordMalformed();
                handler.onMalformedRow(stats.rows(), snippet(buffer, from, to),
                        new IllegalArgumentException("row has no messageId field"));
            } else {
                stats.recordUnknown();
                handler.onUnknownMessageId(messageId, stats.rows());
            }
            return;
        }

        int id = type.id();
        byte want = wanted[id];
        if (want == WANT_UNDECIDED) {
            want = handler.wants(type) ? WANT_YES : WANT_NO;
            wanted[id] = want;
        }
        if (want == WANT_NO) {
            stats.recordSkipped();
            return;
        }

        try {
            MarketMessage message = readersByMessageId[id].readValue(buffer, from, to - from);
            stats.recordMaterialized();
            handler.onMessage(type, message);
        } catch (JacksonException e) {
            stats.recordMalformed();
            handler.onMalformedRow(stats.rows(), snippet(buffer, from, to), e);
        }
    }

    private static boolean isSkippableWhitespace(byte b) {
        return b == '\r' || b == ' ' || b == '\t';
    }

    private static int indexOfNewline(byte[] buffer, int from, int to) {
        for (int i = from; i < to; i++) {
            if (buffer[i] == '\n') {
                return i;
            }
        }
        return -1;
    }

    /**
     * Finds the {@code messageId} key in the raw row and parses the digits after it, without
     * decoding the row. The scan anchors on the key's first letter rather than its opening quote,
     * because quotes are by far the most common byte in a JSON object and would make the inner
     * comparison run on nearly every position.
     *
     * @return the id, or {@code -1} when the field is absent
     */
    private static int readMessageId(byte[] buffer, int from, int to) {
        final int last = to - MESSAGE_ID_KEY.length;
        final byte anchor = MESSAGE_ID_KEY[1];
        outer:
        for (int i = from; i <= last; i++) {
            if (buffer[i + 1] != anchor) {
                continue;
            }
            for (int j = 0; j < MESSAGE_ID_KEY.length; j++) {
                if (buffer[i + j] != MESSAGE_ID_KEY[j]) {
                    continue outer;
                }
            }
            int cursor = i + MESSAGE_ID_KEY.length;
            int value = 0;
            boolean sawDigit = false;
            while (cursor < to) {
                byte c = buffer[cursor];
                if (c < '0' || c > '9') {
                    break;
                }
                value = value * 10 + (c - '0');
                sawDigit = true;
                cursor++;
            }
            return sawDigit ? value : -1;
        }
        return -1;
    }

    private static String snippet(byte[] buffer, int from, int to) {
        return new String(buffer, from, Math.min(to - from, SNIPPET_LIMIT), StandardCharsets.UTF_8);
    }
}
