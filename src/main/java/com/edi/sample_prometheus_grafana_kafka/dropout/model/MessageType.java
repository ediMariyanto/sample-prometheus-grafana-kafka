package com.edi.sample_prometheus_grafana_kafka.dropout.model;

/**
 * Maps the {@code messageId} on the wire to the record that models it.
 *
 * <p>Lookup is an array index rather than a map so the hot parse loop never hashes.
 */
public enum MessageType {

    ORDER(1, OrderMessage.class),
    ORDER_BOOK_DIRECTORY(2, OrderBook.class),
    ASSET_DIRECTORY(3, Asset.class),
    PARTICIPANT_DIRECTORY(4, Participant.class),
    ACTOR_DIRECTORY(5, Actor.class),
    DIRECTORY_END(6, DirectoryEnd.class),
    PRICE_LIMIT(7, PriceLimit.class),
    INDEX_PRICE(9, IndexPrice.class),
    REFERENCE_PRICE(10, ReferencePrice.class),
    EQUILIBRIUM_PRICE(11, EquilibriumPrice.class),
    ORDER_REJECT(14, OrderReject.class),
    TRADING_SESSION(15, TradingSession.class),
    BUSINESS_DATE(17, BusinessDate.class),
    TRANSACTION_BEGIN(18, TransactionBegin.class),
    TRANSACTION_END(19, TransactionEnd.class),
    TRADE(20, Trade.class),
    TOP_OF_BOOK(22, TopOfBook.class),
    MARKET_STATISTICS(26, MarketStatistics.class),
    INDEX_COMPOSITION(101, IndexComposition.class);

    /** Highest {@code messageId} known, used to size the dispatch arrays. */
    public static final int MAX_ID = 101;

    private static final MessageType[] BY_ID = new MessageType[MAX_ID + 1];

    static {
        for (MessageType type : values()) {
            BY_ID[type.id] = type;
        }
    }

    private final int id;
    private final Class<? extends MarketMessage> messageClass;

    MessageType(int id, Class<? extends MarketMessage> messageClass) {
        this.id = id;
        this.messageClass = messageClass;
    }

    /** Returns the type for a raw {@code messageId}, or {@code null} when the id is not known. */
    public static MessageType byId(int messageId) {
        return (messageId >= 0 && messageId <= MAX_ID) ? BY_ID[messageId] : null;
    }

    /**
     * Resolves a type from either its name ({@code TRADE}, case-insensitive) or its raw
     * {@code messageId} ({@code 20}), which is how the wire identifies it.
     *
     * @throws IllegalArgumentException when the token matches neither
     */
    public static MessageType parse(String token) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("message type is required");
        }
        String trimmed = token.trim();
        try {
            return valueOf(trimmed.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            // fall through to the numeric form
        }
        try {
            MessageType byId = byId(Integer.parseInt(trimmed));
            if (byId != null) {
                return byId;
            }
        } catch (NumberFormatException ignored) {
            // reported below with the same message as an unknown name
        }
        throw new IllegalArgumentException("unknown message type: " + token);
    }

    public int id() {
        return id;
    }

    public Class<? extends MarketMessage> messageClass() {
        return messageClass;
    }
}
