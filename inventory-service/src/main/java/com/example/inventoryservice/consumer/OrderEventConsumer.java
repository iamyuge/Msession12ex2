package com.example.inventoryservice.consumer;

import com.example.inventoryservice.dto.OrderCreatedEvent;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Component
public class OrderEventConsumer {

    private final RedissonClient redissonClient;

    private final ConcurrentMap<Long, Integer> stock =
            new ConcurrentHashMap<>();

    public OrderEventConsumer(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;

        stock.put(1L, 1);
        stock.put(2L, 50);
        stock.put(3L, 200);
    }

    @KafkaListener(
            topics = "order-events",
            groupId = "inventory-group"
    )
    public void consume(OrderCreatedEvent event) {

        Long productId = event.getProductId();
        Integer quantity = event.getQuantity();

        if (productId == null || quantity == null || quantity <= 0) {
            System.out.println("Thông tin đặt hàng không hợp lệ.");
            return;
        }

        String lockKey = "lock:product:" + productId;
        RLock lock = redissonClient.getLock(lockKey);
        boolean locked = false;

        try {
            locked = lock.tryLock(3, TimeUnit.SECONDS);

            if (!locked) {
                System.out.println(
                        "Sản phẩm đang được xử lý, vui lòng thử lại. ID = "
                                + productId
                );
                return;
            }

            System.out.println(
                    "Đã lấy khóa: " + lockKey
            );

            Integer currentStock = stock.get(productId);

            if (currentStock == null) {
                System.out.println(
                        "Không tìm thấy sản phẩm ID = " + productId
                );
                return;
            }

            if (currentStock < quantity) {
                System.out.println(
                        "Sản phẩm đã hết hàng. ID = " + productId
                );
                return;
            }

            stock.put(productId, currentStock - quantity);

            System.out.println(
                    "Đặt hàng thành công. Order ID = "
                            + event.getOrderId()
            );
            System.out.println(
                    "Sản phẩm ID = " + productId
            );
            System.out.println(
                    "Số lượng đã mua = " + quantity
            );
            System.out.println(
                    "Tồn kho còn lại = " + stock.get(productId)
            );

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            System.out.println(
                    "Đã gián đoạn khi chờ khóa sản phẩm ID = "
                            + productId
            );

        } finally {
            if (locked && lock.isHeldByCurrentThread()) {
                lock.unlock();

                System.out.println(
                        "Đã giải phóng khóa: " + lockKey
                );
            }
        }
    }
}