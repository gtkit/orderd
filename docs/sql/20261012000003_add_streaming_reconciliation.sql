-- 支付+退款对账：新增源证据、比较发布代次、持久唤醒和正向补偿重试。
-- 上线顺序：停对账worker，执行DDL，全体资金写入实例升级并排空旧事务，再开历史回填。
-- 旧V1账单/差异/人工处置完整保留；新流水同事务追加队列，不依赖MAX(id)提交水位。
-- 大表DDL与旧差异身份回填须事先评估锁窗口；不能在生产直接自动执行。
-- 回滚：先停对账/补偿，保留新增表、列与已确认资金/审计；不DROP或回写旧资金。

ALTER TABLE app_pay_accounts
    ADD COLUMN reconcile_scan_version SMALLINT NOT NULL DEFAULT 1 COMMENT '历史建批次游标算法版本;升级后从created_at重扫',
    ADD COLUMN reconcile_local_seed_cursor BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '历史本地流水回填ID;新写入同事务队列覆盖晚提交';

ALTER TABLE reconcile_batches
    ADD COLUMN algorithm_version SMALLINT NOT NULL DEFAULT 1 COMMENT '内部算法代次;1历史支付,2支付与退款',
    ADD COLUMN run_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '当前比较代次',
    ADD COLUMN source_run_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '当前完整源导入代次,重比沿用',
    ADD COLUMN published_run_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '对外可见完整比较代次',
    ADD COLUMN published_source_run_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '对外可见完整账单源代次',
    ADD COLUMN import_complete BOOLEAN NOT NULL DEFAULT FALSE COMMENT '完整性与汇总校验成功后封存',
    ADD COLUMN compare_after_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '源证据逐页比较检查点',
    ADD COLUMN dirty_seq BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '迟到本地资金触发的重比序号',
    ADD COLUMN run_dirty_seq BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当前比较已接收的重比序号',
    ADD COLUMN published_dirty_seq BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '已发布比较覆盖的重比序号',
    ADD COLUMN source_digest_algorithm VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '源文件审计摘要算法,旧记录留空',
    ADD COLUMN source_digest VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '源文件摘要,仅审计不替代渠道成功复核',
    ADD COLUMN source_refund_amount BIGINT NOT NULL DEFAULT 0 COMMENT '渠道退款本金合计,分',
    ADD COLUMN local_refund_amount BIGINT NOT NULL DEFAULT 0 COMMENT '匹配范围内本地退款本金合计,分';

ALTER TABLE reconcile_diffs
    ADD COLUMN run_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '比较代次;空为旧算法或本地单边差异',
    ADD COLUMN entry_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '源证据ID;0禁止自动资金补偿',
    ADD COLUMN kind TINYINT NOT NULL DEFAULT 1 COMMENT '1支付 2退款',
    ADD COLUMN identity_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '完整资金身份SHA256;本地单边按流水ID',
    ADD COLUMN local_txn_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '本地不可变资金流水ID',
    ADD COLUMN original_transaction_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '退款原支付渠道交易号',
    ADD COLUMN out_refund_no VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '商户退款请求号',
    ADD COLUMN refund_no VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '本地退款单号',
    ADD COLUMN currency VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '核对币种;旧差异未知留空',
    ADD COLUMN verify_attempts INT NOT NULL DEFAULT 0 COMMENT '正向核实累计次数,不因重试清零',
    ADD COLUMN next_verify_at DATETIME(3) NULL DEFAULT NULL COMMENT '正向补偿下次可尝试时间;NULL首次可领取',
    ADD COLUMN verify_locked_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '每次补偿认领随机token',
    ADD COLUMN verify_locked_until DATETIME(3) NULL DEFAULT NULL COMMENT '补偿租约截止,过期结果禁止写回',
    ADD COLUMN verify_last_error TEXT NULL COMMENT '脱敏补偿结果,不写下载URL或原始响应';

ALTER TABLE reconcile_batches
    DROP INDEX uk_account_date,
    ADD UNIQUE KEY uk_account_date (channel, pay_account_id, bill_date, algorithm_version),
    DROP INDEX idx_reconcile_claim,
    ADD KEY idx_reconcile_claim (channel, pay_account_id, algorithm_version, status, locked_until, id);

-- 复刻旧唯一身份(channel,transaction_id)，不让全部旧行使用空identity导致新唯一键碰撞。
UPDATE reconcile_diffs
    SET identity_key = SHA2(CONCAT(channel, CHAR(0), transaction_id), 256)
    WHERE identity_key = '';
ALTER TABLE reconcile_diffs
    DROP INDEX uk_batch_txn,
    ADD UNIQUE KEY uk_batch_txn (batch_id, run_id, kind, identity_key, diff_type),
    ADD KEY idx_repair_claim (channel, pay_account_id, handle_status, next_verify_at, verify_locked_until, id),
    ADD KEY idx_local_diff (batch_id, local_txn_id);
ALTER TABLE payment_transactions ADD KEY idx_account_seed (channel, pay_account_id, id);

CREATE TABLE reconcile_bill_entries (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    batch_id BIGINT UNSIGNED NOT NULL,
    run_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    kind TINYINT NOT NULL,
    identity_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    channel VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    pay_account_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    transaction_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '独立渠道交易或退款号;支付宝退款为空',
    original_transaction_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    out_trade_no VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    out_refund_no VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    amount BIGINT NOT NULL COMMENT '本金分,包含券等资金组成,不使用净结算金额',
    currency VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_at DATETIME(3) NULL COMMENT '渠道事件时间;退款可能为受理日,不能作为成功日',
    merchant_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    provider_app_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '文件真实AppID;支付宝无此列留空',
    source_status VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_source_identity (batch_id, run_id, kind, identity_key),
    KEY idx_source_page (batch_id, run_id, id),
    KEY idx_source_match (channel, pay_account_id, kind, identity_key, batch_id),
    CONSTRAINT chk_bill_entry_kind CHECK (kind IN (1,2)),
    CONSTRAINT chk_bill_entry_amount CHECK (amount > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE reconcile_local_changes (
    txn_id BIGINT UNSIGNED NOT NULL COMMENT '同资金Insert事务创建,无外键或聚合锁',
    channel VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    pay_account_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    done BOOLEAN NOT NULL DEFAULT FALSE COMMENT '已有完整匹配证据后停止重试',
    attempts INT NOT NULL DEFAULT 0,
    next_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    locked_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    locked_until DATETIME(3) NULL DEFAULT NULL,
    last_error TEXT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (txn_id),
    KEY idx_local_claim (channel, pay_account_id, done, next_at, locked_until, txn_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
