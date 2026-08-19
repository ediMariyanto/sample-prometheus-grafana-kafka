package com.edi.sample_prometheus_grafana_kafka.controller;


import com.edi.sample_prometheus_grafana_kafka.dto.OrderRequest;
import com.edi.sample_prometheus_grafana_kafka.dto.OrderResponse;
import com.edi.sample_prometheus_grafana_kafka.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/main")
public class OrderController {

    @Autowired
    private OrderService orderService;

    @PostMapping("/order")
    public ResponseEntity<OrderResponse> order(@RequestBody OrderRequest order) {
        return ResponseEntity.ok().body(orderService.order(order));
    }

}
