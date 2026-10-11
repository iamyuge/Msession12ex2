package com.example.emailservice.consumer;

import com.example.emailservice.dto.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    @KafkaListener(
            topics = "order-events",
            groupId = "email-group"
    )
    public void consume(OrderCreatedEvent event) {

        System.out.println("===== EMAIL SERVICE =====");
        System.out.println(
                "Đang xử lý email cho đơn hàng: "
                        + event.getOrderId()
        );
        System.out.println(
                "Email khách hàng: "
                        + event.getCustomerEmail()
        );
        System.out.println(
                "Tổng tiền: "
                        + event.getTotalAmount()
        );
        System.out.println(
                "Đã xử lý yêu cầu email xác nhận đơn hàng."
        );
    }
}