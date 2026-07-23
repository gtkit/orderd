-- =====================================================================
-- 接入方侧 inbox 表(不在 orderd 库,属于接入方自己的业务库)。
--
-- 这是评审的第 1 号上线阻断项的落地:权益发放的幂等真相源必须是接入方 MySQL,
-- 不能用 Redis。消费 order.paid webhook 时,在【同一个业务数据库事务】内完成:
--
--   BEGIN;
--     INSERT INTO inbox_events(event_id, order_no, event_type, aggregate_version)  -- 唯一键防重
--       VALUES(?, ?, ?, ?);            -- 冲突 = 重复事件,回滚并直接回 2xx(已处理)
--     -- 按 (order_no, aggregate_version) 校验单调,拒绝比已处理版本更旧的事件;
--     UPDATE member_account ...        -- 发放/回退会员权益
--     INSERT INTO benefit_ledger ...   -- 记权益流水(可审计)
--   COMMIT;                            -- 提交成功后才对 webhook 回 2xx
--
-- Redis 只能做"快速判重"加速,不能作为权威幂等存储:Redis 丢失/过期/淘汰后,
-- 重复事件会再次发权益;先写 Redis 再发权益,进程崩溃会永久漏发。
-- =====================================================================
CREATE TABLE inbox_events (
    event_id          VARCHAR(36) NOT NULL COMMENT 'orderd 事件ID,幂等唯一键',
    order_no          VARCHAR(64) NOT NULL COMMENT '订单号',
    event_type        VARCHAR(32) NOT NULL COMMENT 'order.paid/refunded/closed',
    aggregate_version BIGINT      NOT NULL COMMENT '事件聚合版本(防乱序回退)',
    processed_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '处理时间',
    PRIMARY KEY (event_id),
    KEY idx_order_ver (order_no, aggregate_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '接入方事件收件箱(权益发放幂等真相源)';
