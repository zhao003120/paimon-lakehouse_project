package com.example.paimon.common;

import java.math.BigDecimal;
import java.io.Serializable;

/**
 * 订单事件 POJO。
 *
 * 模拟从 Kafka 消费的订单数据，可能包含脏数据：
 *   - amount 为负数
 *   - amount 异常大
 *   - amount 为 null
 *
 * 对应文章坑三：Flink 从 Kafka 直写 Paimon，中间没校验，
 * 脏数据把 AVG 聚合带歪了 47 万。
 */
public class OrderEvent implements Serializable {

    private String orderId;
    private BigDecimal amount;
    private String dt;

    public OrderEvent() {}

    public OrderEvent(String orderId, BigDecimal amount, String dt) {
        this.orderId = orderId;
        this.amount = amount;
        this.dt = dt;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }

    public String getDt() { return dt; }
    public void setDt(String dt) { this.dt = dt; }

    @Override
    public String toString() {
        return String.format("OrderEvent{orderId='%s', amount=%s, dt='%s'}", orderId, amount, dt);
    }
}
