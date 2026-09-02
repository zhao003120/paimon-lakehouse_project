package com.example.paimon.common;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据校验器（坑三的核心修复逻辑）。
 *
 * 文章原文：
 *   "一句话的根因：Flink 从 Kafka 直写 Paimon，中间就没校验。"
 *   "AVG(amount) 本来 1000 上下，硬给拉到 1500。"
 *
 * 修复方案：
 *   1. Flink 层过滤：负数、异常大值、null 直接拦截
 *   2. 脏数据单独记录：写入 dirty_orders 表，方便追溯
 */
public class DataValidator {

    private static final BigDecimal MAX_AMOUNT = new BigDecimal(PaimonConfig.MAX_AMOUNT);
    private static final BigDecimal MIN_AMOUNT = new BigDecimal(PaimonConfig.MIN_AMOUNT);

    public enum ValidationResult {
        VALID,
        NULL_AMOUNT,
        NEGATIVE_AMOUNT,
        ABNORMAL_AMOUNT
    }

    /**
     * 校验订单数据。
     *
     * @param order 待校验的订单
     * @return 校验结果
     */
    public static ValidationResult validate(OrderEvent order) {
        if (order.getAmount() == null) {
            return ValidationResult.NULL_AMOUNT;
        }
        if (order.getAmount().signum() < 0) {
            return ValidationResult.NEGATIVE_AMOUNT;
        }
        if (order.getAmount().compareTo(MAX_AMOUNT) > 0) {
            return ValidationResult.ABNORMAL_AMOUNT;
        }
        return ValidationResult.VALID;
    }

    /**
     * 生成脏数据记录，写入 dirty_orders 表。
     */
    public static Map<String, Object> toDirtyRecord(OrderEvent order, ValidationResult reason) {
        Map<String, Object> record = new HashMap<>();
        record.put("order_id", order.getOrderId() != null ? order.getOrderId() : "UNKNOWN");
        record.put("amount", order.getAmount() != null ? order.getAmount().toPlainString() : "null");
        record.put("dt", order.getDt() != null ? order.getDt() : "UNKNOWN");
        record.put("reject_reason", reason.name());
        record.put("raw_data", order.toString());
        return record;
    }
}
