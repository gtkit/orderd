-- 退款查询证据迁移。现有项目没有统一 migration runtime,此文件由发布流程逐步执行。
-- 部署顺序:停止旧退款执行/查询写入→加列→保守回填→校验→统一启动新版本,禁止新旧实例混跑。
-- 编号只执行一次;失败重试前用 information_schema.columns 检查新增列是否已存在,确认回填完成。
-- 大表执行 ALTER 需先评估锁表窗口;本变更不改历史资金流水、不重新提交渠道退款。

-- up
ALTER TABLE refunds
    ADD COLUMN not_exist_count INT NOT NULL DEFAULT 0
        COMMENT '连续明确退款不存在次数,非不存在结果或查询错误清零,不用于退避' AFTER gateway_refund_id,
    ADD COLUMN gateway_confirmed TINYINT NOT NULL DEFAULT 0
        COMMENT '渠道已受理或查到存在(历史在途保守保护),0否1是,确认后不可清零' AFTER not_exist_count,
    ADD CONSTRAINT chk_refund_not_exist_count CHECK (not_exist_count >= 0),
    ADD CONSTRAINT chk_refund_gateway_confirmed CHECK (gateway_confirmed IN (0, 1));

-- 旧在途模型无法区分受理成功/结果未知,保守保护;旧 query_count 不作为连续否定证据。
UPDATE refunds SET gateway_confirmed = 1
WHERE status IN (0, 1, 5) OR gateway_refund_id <> '';

-- 校验:上述条件行的 gateway_confirmed 必须为1;新计数初始应为0。
-- 回滚应用必须先停止退款写入和worker,推荐保留新增列,避免丢失否定/正面证据。
-- down(仅所有依赖新字段的实例停止并已保存证据后,由发布流程显式执行):
-- ALTER TABLE refunds
--     DROP CHECK chk_refund_not_exist_count,
--     DROP CHECK chk_refund_gateway_confirmed,
--     DROP COLUMN gateway_confirmed,
--     DROP COLUMN not_exist_count;
