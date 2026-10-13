-- 对账历史恢复游标与后台认领索引;先迁移,再部署新 worker。
-- 本项目无自动迁移 runtime。由 DBA/发布流程评审并按迁移编号执行一次;
-- 不对历史账号批量回填,NULL 由应用从现有 created_at 日起分批推进。
-- 大表加索引须先评估执行计划、DDL 锁和发布窗口;本任务不执行生产 DDL。

ALTER TABLE app_pay_accounts
    ADD COLUMN reconcile_scanned_through DATE NULL DEFAULT NULL
    COMMENT '已确保连续账期批次存在的游标;NULL从created_at日起步,不是完成水位';

ALTER TABLE reconcile_batches
    ADD INDEX idx_reconcile_claim (channel, pay_account_id, status, locked_until, id),
    MODIFY COLUMN locked_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''
    COMMENT '本次认领随机标识(每次重新生成,完成与失败写回CAS)',
    MODIFY COLUMN locked_until DATETIME NULL
    COMMENT '执行租约到期;失败且无持有者时为下次可重试时间';

ALTER TABLE orders
    ADD INDEX idx_closing_scan (status, updated_at, id);

ALTER TABLE event_outbox
    MODIFY COLUMN locked_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''
    COMMENT '本次认领随机标识(每次重新生成,失败写回CAS)';

-- 等价回滚:停止新 worker 并回滚应用,保留新增列/索引及所有batch/diff/资金数据。
-- 不自动执行破坏性的结构 down。仅显式授权并确认不再需要恢复进度时,由发布流程执行:
-- ALTER TABLE app_pay_accounts DROP COLUMN reconcile_scanned_through;
-- ALTER TABLE reconcile_batches DROP INDEX idx_reconcile_claim;
-- ALTER TABLE orders DROP INDEX idx_closing_scan;
-- 可恢复旧列COMMENT;不得删除批次、差异或资金流水。
