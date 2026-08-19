package com.edi.sample_prometheus_grafana_kafka.dropout.index;

import com.edi.sample_prometheus_grafana_kafka.dropout.model.EquilibriumPrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.IndexPrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.MarketStatistics;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.OrderBook;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.PriceLimit;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.ReferencePrice;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.TopOfBook;
import com.edi.sample_prometheus_grafana_kafka.dropout.model.TradingSession;

/**
 * An order book joined to the last state seen for it in the file. Any component is {@code null}
 * when the file carried no such message for that book.
 */
public record OrderBookView(
        OrderBook orderBook,
        TradingSession session,
        PriceLimit priceLimit,
        ReferencePrice referencePrice,
        EquilibriumPrice equilibrium,
        TopOfBook topOfBook,
        MarketStatistics statistics,
        IndexPrice indexPrice,
        int tradeCount
) {
}
