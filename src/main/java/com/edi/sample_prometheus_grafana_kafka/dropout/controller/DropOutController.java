package com.edi.sample_prometheus_grafana_kafka.dropout.controller;

import com.edi.sample_prometheus_grafana_kafka.dropout.dto.LoadSummary;
import com.edi.sample_prometheus_grafana_kafka.dropout.dto.MessageQuery;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.DropOutIndex;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.InstrumentSnapshot;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.FieldSpec;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.MessagePage;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.MessageSchema;
import com.edi.sample_prometheus_grafana_kafka.dropout.index.OrderBookView;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Actor;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Asset;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.IndexComposition;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MessageType;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderMessage;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderReject;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Participant;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.Trade;
import com.edi.sample_prometheus_grafana_kafka.dropout.service.DropOutLookupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.util.MultiValueMap;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Upload a drop-copy file and query it.
 *
 * <p>The upload is streamed straight into the parser, so a multi-hundred-megabyte file never lands
 * in memory as a byte array. Only one file is held at a time - a new upload replaces the previous
 * index.
 */
@RestController
@RequestMapping("/v1/dropout")
public class DropOutController {

    Logger log = LoggerFactory.getLogger(DropOutController.class);

    private final DropOutLookupService lookupService;

    public DropOutController(DropOutLookupService lookupService) {
        this.lookupService = lookupService;
    }

    /**
     * {@code POST /v1/dropout/upload} with multipart field {@code file}.
     *
     * <p>{@code skip} optionally names message types to leave out, by name or numeric messageId,
     * e.g. {@code ?skip=TRANSACTION_BEGIN,TRANSACTION_END}. Skipping is faster and lighter but
     * removes those types from {@code /messages}; the response echoes what was skipped.
     */
    @PostMapping(path = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<LoadSummary> upload(@RequestParam("file") MultipartFile file,
                                              @RequestParam(name = "skip", required = false) List<String> skip)
            throws IOException {
        long start = System.currentTimeMillis();
        log.info("Uploading file start {}", file.getOriginalFilename());
        if (file.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "file is empty");
        }
        Set<MessageType> skipped = parseTypes(skip);
        try (InputStream in = file.getInputStream()) {
            String name = Optional.ofNullable(file.getOriginalFilename()).orElse("upload");
            LoadSummary summary = lookupService.load(name, in, skipped);
            log.info("Uploading file finished .... ");
            log.info("length of time call rpc => oms.upload_batch_order-rpc: " + (System.currentTimeMillis() - start) + " in ms");
            return ResponseEntity.ok(summary);
        } catch (RuntimeException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "could not parse file: " + e.getMessage(), e);
        }

    }

    /** Message types available to filter, with how many rows of each are retained. */
    @GetMapping("/messages")
    public Map<String, Object> messageTypes() {
        DropOutIndex current = index();
        Map<String, Integer> retained = new LinkedHashMap<>();
        current.retainedCounts().forEach((type, count) -> retained.put(type.name(), count));

        Map<String, Integer> messageIds = new LinkedHashMap<>();
        for (MessageType type : MessageType.values()) {
            messageIds.put(type.name(), type.id());
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("retainedByType", retained);
        body.put("skippedTypes", current.skippedTypes().stream().map(MessageType::name).toList());
        body.put("messageIds", messageIds);
        return body;
    }

    /**
     * {@code GET /v1/dropout/messages/{type}} - filters the rows of one message type and returns
     * them in that type's own shape.
     *
     * <p>{@code type} is either the name ({@code TRADE}) or the raw messageId ({@code 20}). Every
     * filter parameter is optional and they combine with AND; see {@link MessageQuery}.
     */
    @GetMapping("/messages/{type}")
    public MessagePage messages(@PathVariable String type,
                                @ModelAttribute MessageQuery query,
                                @RequestParam MultiValueMap<String, String> allParams) {
        return lookupService.query(MessageType.parse(type), query, allParams);
    }

    /**
     * The field layout of one structure: everything that can be filtered on it, under both its Java
     * name and its name in the file. This is what {@code messageId} discriminates, made explicit.
     */
    @GetMapping("/messages/{type}/fields")
    public Map<String, Object> fields(@PathVariable String type) {
        MessageType messageType = MessageType.parse(type);
        MessageSchema schema = MessageSchema.of(messageType);

        List<Map<String, Object>> fields = new ArrayList<>();
        for (FieldSpec spec : schema.fields()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", spec.name());
            entry.put("wireName", spec.wireName());
            entry.put("kind", spec.kind().name());
            entry.put("filterable", spec.filterable());
            fields.add(entry);
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("type", messageType.name());
        body.put("messageId", messageType.id());
        body.put("structure", messageType.messageClass().getSimpleName());
        body.put("fieldCount", schema.fields().size());
        body.put("fields", fields);
        body.put("reservedParams", MessageQuery.RESERVED_PARAMS.stream().sorted().toList());
        return body;
    }

    private static Set<MessageType> parseTypes(List<String> tokens) {
        if (tokens == null || tokens.isEmpty()) {
            return Set.of();
        }
        Set<MessageType> types = EnumSet.noneOf(MessageType.class);
        for (String token : tokens) {
            for (String part : token.split(",")) {
                if (!part.isBlank()) {
                    types.add(MessageType.parse(part));
                }
            }
        }
        return types;
    }

    /** Unknown message type, or an unparseable timestamp - the request is at fault, not the server. */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
    }

    /** Raised by the service when a query arrives before any file has been loaded. */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> conflict(IllegalStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", String.valueOf(e.getMessage())));
    }

    /** Result of the most recent upload. */
    @GetMapping("/summary")
    public LoadSummary summary() {
        return lookupService.lastSummary()
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.CONFLICT, "no file loaded yet"));
    }

    /** Resolves a ticker, ISIN, order book name or numeric id to its instrument and market state. */
    @GetMapping("/lookup/{query}")
    public InstrumentSnapshot lookup(@PathVariable String query) {
        return lookupService.lookup(query)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "no instrument matches " + query));
    }

    @GetMapping("/order-books/{orderBookId}")
    public OrderBookView orderBook(@PathVariable long orderBookId) {
        return require(index -> index.orderBookView(orderBookId), "order book " + orderBookId);
    }

    @GetMapping("/order-books/{orderBookId}/trades")
    public List<Trade> trades(@PathVariable long orderBookId) {
        return index().trades(orderBookId);
    }

    @GetMapping("/assets/{assetId}")
    public Asset asset(@PathVariable long assetId) {
        return require(index -> index.asset(assetId), "asset " + assetId);
    }

    @GetMapping("/orders/{orderId}")
    public OrderMessage order(@PathVariable long orderId) {
        return require(index -> index.order(orderId), "order " + orderId);
    }

    @GetMapping("/participants/{participantId}")
    public Participant participant(@PathVariable long participantId) {
        return require(index -> index.participant(participantId), "participant " + participantId);
    }

    @GetMapping("/actors/{actorId}")
    public Actor actor(@PathVariable long actorId) {
        return require(index -> index.actor(actorId), "actor " + actorId);
    }

    /** Constituents and weights of an index order book. */
    @GetMapping("/indices/{indexOrderBookId}/members")
    public List<IndexComposition> indexMembers(@PathVariable long indexOrderBookId) {
        return index().indexMembers(indexOrderBookId);
    }

    @GetMapping("/rejects")
    public List<OrderReject> rejects() {
        return index().rejects();
    }

    private <T> T require(Function<DropOutIndex, Optional<T>> accessor, String what) {
        return accessor.apply(index())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, what + " not found"));
    }

    private DropOutIndex index() {
        return lookupService.index()
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.CONFLICT, "no file loaded yet"));
    }
}
