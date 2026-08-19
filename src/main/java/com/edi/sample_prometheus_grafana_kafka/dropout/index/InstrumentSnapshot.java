package com.edi.sample_prometheus_grafana_kafka.dropout.index;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.Asset;

import java.util.List;

/**
 * Result of a lookup: the asset that was resolved, how it was matched, and its order books with
 * their latest state.
 *
 * <p>When the query matched an asset (ticker or ISIN) every book of that asset is returned; when it
 * matched a single book by name or id, only that book is.
 */
public record InstrumentSnapshot(
        String query,
        MatchedBy matchedBy,
        Asset asset,
        List<OrderBookView> orderBooks
) {

    public enum MatchedBy {
        ASSET_ID,
        ASSET_NAME,
        ISIN,
        ORDER_BOOK_ID,
        ORDER_BOOK_NAME
    }
}
