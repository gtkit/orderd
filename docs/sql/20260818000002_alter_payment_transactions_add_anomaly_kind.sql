-- 增量草稿(人工评审执行;生产禁 AutoMigrate):payment_transactions 加 anomaly_kind。
--
-- 成因落库:anomaly=1 只说明"这笔有问题",三种成因的处置方式不同(第二笔真实付款→退回,
-- 金额不符→核对渠道账单,账号错配→查配置),不落库则每次都要翻告警日志反推(DESIGN 评审 D6)。
-- 历史行该列为空串=成因未随流水落库(列上线前写入的数据)。
--
-- ⚠ 执行要求:表尾加列 + NOT NULL DEFAULT '' + 无字符集变更,满足 MySQL 8 INSTANT ADD COLUMN;
-- 必须显式 ALGORITHM=INSTANT 执行(不满足条件时报错而非退化为拷表),并在低峰验证。
ALTER TABLE payment_transactions
    ADD COLUMN anomaly_kind VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '异常成因(second_real_payment/amount_mismatch/account_channel_mismatch;空=成因未落库的历史行)',
    ALGORITHM = INSTANT;
