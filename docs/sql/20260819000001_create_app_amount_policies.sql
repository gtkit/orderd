-- 增量草稿(人工评审执行;生产禁 AutoMigrate):新建 app_amount_policies。
--
-- per-app 金额策略(实付上下限/业务类型白名单/币种白名单)。只拦异常值,金额真实性
-- 由 PricingAuthority 边界保证。未配置或 enabled=0 的 app 回退全局默认档(应用层回退链)。
-- 白名单为逗号分隔文本(集合极小,低频读):allowed_business 空=不限;allowed_currency 空=仅 CNY。
CREATE TABLE app_amount_policies (
    app_id           VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '接入方标识',
    min_payable      BIGINT NOT NULL DEFAULT 0 COMMENT '实付下限(分,含),0=不限',
    max_payable      BIGINT NOT NULL DEFAULT 0 COMMENT '实付上限(分,含),0=不限',
    allowed_business VARCHAR(512) NOT NULL DEFAULT '' COMMENT '业务类型白名单(逗号分隔,空=不限)',
    allowed_currency VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '币种白名单(逗号分隔,空=仅CNY)',
    enabled          TINYINT NOT NULL DEFAULT 1 COMMENT '是否启用(0=回退全局默认档)',
    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (app_id),
    CONSTRAINT chk_amount_policy_nonneg CHECK (min_payable >= 0 AND max_payable >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='per-app 金额策略(未配置回退全局默认档)';
