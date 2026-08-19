package com.edi.sample_prometheus_grafana_kafka.service;

import com.edi.sample_prometheus_grafana_kafka.dto.OrderRequest;
import com.edi.sample_prometheus_grafana_kafka.dto.OrderResponse;
import com.edi.sample_prometheus_grafana_kafka.model.OrderType;
import com.edi.sample_prometheus_grafana_kafka.model.Status;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderService {
    Logger log = LoggerFactory.getLogger(OrderService.class);

    public OrderResponse order(OrderRequest order) {
        log.info("Order received : {} ", order);


        return new OrderResponse(
              "TR-11020-DDV",
              "ED-1234",
                "Edi",
                order.quantity(),
                order.stockCode(),
                OrderType.BUY,
                Status.OPEN
        );
    }
}
