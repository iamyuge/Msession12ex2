package com.example.orderservice.service;

import com.example.orderservice.dto.OrderCreatedEvent;
import com.example.orderservice.entity.Order;
import com.example.orderservice.repository.OrderRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class OrderService {

    private static final String TOPIC = "order-events";

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderService(
            OrderRepository orderRepository,
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public Order createOrder(Order order) throws Exception {

        if (order.getCustomerId() == null
                || order.getProductId() == null
                || order.getQuantity() == null
                || order.getQuantity() <= 0
                || order.getTotalAmount() == null
                || order.getCustomerEmail() == null
                || order.getCustomerEmail().isBlank()) {
            throw new IllegalArgumentException(
                    "Thông tin đặt hàng không hợp lệ"
            );
        }

        Order savedOrder = orderRepository.saveAndFlush(order);

        OrderCreatedEvent event = new OrderCreatedEvent(
                savedOrder.getId(),
                savedOrder.getCustomerId(),
                savedOrder.getCustomerEmail(),
                savedOrder.getProductId(),
                savedOrder.getQuantity(),
                savedOrder.getTotalAmount(),
                savedOrder.getOrderDate()
        );

        kafkaTemplate.send(
                TOPIC,
                String.valueOf(savedOrder.getId()),
                event
        ).get(5, TimeUnit.SECONDS);

        System.out.println(
                "Đã gửi OrderCreatedEvent cho đơn hàng "
                        + savedOrder.getId()
        );

        return savedOrder;
    }
}