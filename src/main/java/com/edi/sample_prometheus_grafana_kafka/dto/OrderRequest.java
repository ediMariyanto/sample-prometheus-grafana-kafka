package com.edi.sample_prometheus_grafana_kafka.dto;

public record OrderRequest(
        long accountId,String stockCode, int quantity
) {
}
