-- 按异常流水退款(RefundAnomalyTxn):refunds 增加源流水指针,活跃守卫按退款来源区分。
-- 存量行 source_txn_id 为 NULL,守卫值仍为 order_no,无需回填。
-- MODIFY STORED 生成列会重建表,refunds 行数小,低峰执行。

-- up
ALTER TABLE refunds
    ADD COLUMN source_txn_id BIGINT UNSIGNED NULL COMMENT '按异常流水退款的源支付流水 payment_transactions.id(NULL=订单级退款)' AFTER order_no,
    MODIFY COLUMN active_guard VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin
        GENERATED ALWAYS AS (CASE WHEN status IN (0, 1, 5)
          THEN IF(source_txn_id IS NULL, order_no, CONCAT('txn:', source_txn_id)) ELSE NULL END) STORED,
    ADD KEY idx_source_txn (source_txn_id);

-- down(须先确认不存在 source_txn_id 非空的退款)
-- ALTER TABLE refunds
--     MODIFY COLUMN active_guard VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin
--         GENERATED ALWAYS AS (CASE WHEN status IN (0, 1, 5) THEN order_no ELSE NULL END) STORED,
--     DROP KEY idx_source_txn,
--     DROP COLUMN source_txn_id;
