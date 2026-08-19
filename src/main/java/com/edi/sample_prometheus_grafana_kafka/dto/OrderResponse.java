package com.edi.sample_prometheus_grafana_kafka.dto;

import com.edi.sample_prometheus_grafana_kafka.model.OrderType;
import com.edi.sample_prometheus_grafana_kafka.model.Status;

public record OrderResponse(
        String transCode
        , String accountCode
        , String accountName
        , int quantity
        , String stockCode
        , OrderType orderType
        , Status status
){
}
