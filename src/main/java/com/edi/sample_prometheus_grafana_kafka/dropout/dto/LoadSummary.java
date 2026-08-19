package com.edi.sample_prometheus_grafana_kafka.dropout.dto;

import java.util.List;
import java.util.Map;

/** What a single file load produced: throughput of the parse plus the size of the resulting index. */
public record LoadSummary(
        String filename,
        long sizeBytes,
        long rows,
        long materializedRows,
        long skippedRows,
        long unknownRows,
        long malformedRows,
        long elapsedMillis,
        long rowsPerSecond,
        String businessDate,
        IndexCounts index,
        Map<String, Long> rowsByType,
        /* Types deliberately not retained, so their absence from a filter is visible, not silent. */
        List<String> skippedTypes
) {

    public record IndexCounts(
            int assets,
            int orderBooks,
            int participants,
            int actors,
            int orders,
            int trades,
            int rejects
    ) {
    }
}
