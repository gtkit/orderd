-- orderd 权威建表脚本(草稿)。
--
-- 本项目【无迁移工具】:本文件是 schema 的单一权威来源,需人工评审后由 DBA/发布流程执行;
-- 【生产禁用 gorm AutoMigrate】。ormx/gorm 只按本 schema 读写,不据 model 改表。
--
-- 承载 gorm 无法表达的 DB 级不变量与守卫:
--   · 生成列唯一索引 active_order / active_guard(活跃支付尝试 / 活跃退款唯一);
--   · 金额 CHECK(original-discount=pay 等)、ascii_bin collation、多租户唯一键。
-- 由 database/migrations/*.sql 的 up 段汇编而来(迁移工具已移除)。
--
-- 建库示例(按需):CREATE DATABASE orderd DEFAULT CHARSET=utf8mb4; USE orderd;

-- ============================================================
-- from: 20260716000001_create_orders.sql
-- ============================================================
--
-- orders(v3):订单主表。相比 v2 补齐评审阻断项:
--   · request_hash    幂等键绑定归一化请求(同 key 不同请求 → 冲突拒绝);
--   · 金额权威快照     currency + original/discount/pay,带不变量 + 非负约束;
--   · 下单账号快照     pay_account_id + mch_id + app_id + config_version(冻结,查单/关单用);
--   · 成功账号快照     settled_*(支付成功时写入胜出 attempt 的账号,退款必须用它,
--                      而非下单首账号——多 attempt 换号后退款账号才正确,见评审 #6);
--   · aggregate_version 事件版本,每次状态跃迁 +1;
--   · close_reason     关单原因(替代语义重叠的 EXPIRED)。
-- order_token 由服务端生成为安全随机值(非派生);它是订单 bearer 凭证,不进任何事件/webhook。
--
-- collation:身份/幂等/路由类列一律 ascii_bin(区分大小写、按字节比较),
-- 防止默认 utf8mb4_0900_ai_ci 把 'Key' 与 'key'、大小写不同的 order_no/trade_no 误判相等。
CREATE TABLE orders (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    app_id        VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '接入方标识(多租户)',

    idempotency_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT '下单幂等键(NULL=未用;空串必须归一化为 NULL)',
    request_hash    CHAR(64)    CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT '归一化请求哈希,与幂等键绑定,防同键改参',
    client_order_no VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL COMMENT '接入方业务单号(一等字段;NULL 允许多行,空串必须归一化为 NULL)',

    order_no      VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '业务订单号',
    order_token   VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '订单凭证(安全随机,不可预测,不进事件)',
    user_id       BIGINT       NOT NULL COMMENT '用户ID(来自接入方鉴权上下文)',
    status        TINYINT      NOT NULL DEFAULT 0 COMMENT '状态 0待支付 10已支付 30关单中 40关闭 50取消 60退款中 70已退款 80退款异常',
    close_reason  TINYINT      NOT NULL DEFAULT 0 COMMENT '关单原因 0无 1超时 2被取代 3系统 4用户取消',

    product_id       BIGINT UNSIGNED NOT NULL COMMENT '商品ID',
    product_title    VARCHAR(255) NOT NULL COMMENT '商品标题',
    business_type    VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '业务类型(按 app 白名单校验)',
    metadata_type    VARCHAR(64)  NOT NULL DEFAULT '' COMMENT 'metadata 类型标签',
    metadata_version INT          NOT NULL DEFAULT 0 COMMENT 'metadata schema 版本',
    metadata         VARBINARY(4096) NULL COMMENT '接入方业务附加数据(不透明,禁密钥/PII)',

    -- 金额权威快照(单位:分)。不变量:original_price - discount_amount = pay_amount;全部非负,实付为正。
    currency        VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'CNY' COMMENT '币种',
    original_price  BIGINT NOT NULL COMMENT '原价(分)',
    discount_amount BIGINT NOT NULL DEFAULT 0 COMMENT '优惠(分)',
    pay_amount      BIGINT NOT NULL COMMENT '实付(分)',
    refunded_amount BIGINT NOT NULL DEFAULT 0 COMMENT '累计已退(分)',

    -- 下单账号快照(下单时冻结;查单/关单沿用)。
    pay_method     TINYINT     NOT NULL DEFAULT 0 COMMENT '支付方式 1微信 2支付宝',
    pay_channel    VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '渠道标识',
    pay_account_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '下单支付账号ID(冻结)',
    pay_mch_id     VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '下单商户号(冻结)',
    pay_app_id     VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '下单支付应用ID(冻结)',
    config_version INT         NOT NULL DEFAULT 0 COMMENT '下单账号配置版本(冻结)',

    -- 成功账号快照(支付成功事务 §7 内写入胜出 attempt 的账号/渠道/商户单号;退款/退款查单以此为权威,
    -- 换渠道后由第二 attempt 成功时,退款必须用结算渠道而非下单渠道,见评审 P1)。
    settled_attempt_id     BIGINT UNSIGNED NULL COMMENT '胜出支付尝试ID(NULL=尚未支付成功)',
    settled_channel        VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '实际成功渠道(退款用)',
    settled_out_trade_no   VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '实际成功商户订单号(退款用)',
    settled_pay_account_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '实际成功账号ID(退款用)',
    settled_mch_id         VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '实际成功商户号',
    settled_pay_app_id     VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '实际成功支付应用ID',
    settled_config_version INT         NOT NULL DEFAULT 0 COMMENT '实际成功账号配置版本',

    trade_no       VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '第三方交易号',
    paid_at        DATETIME    NULL COMMENT '支付成功时间',
    channel_id     BIGINT      NOT NULL DEFAULT 0 COMMENT '来源渠道ID',

    aggregate_version BIGINT NOT NULL DEFAULT 0 COMMENT '事件版本,每次状态跃迁 +1',
    expire_at         DATETIME NOT NULL COMMENT '支付截止时间',

    client_ip     VARBINARY(16) NULL COMMENT '客户端IP(二进制)',
    client_ip_str VARCHAR(45)  NOT NULL DEFAULT '' COMMENT '客户端IP(字符串)',
    remark        VARCHAR(255) NOT NULL DEFAULT '' COMMENT '用户备注',
    admin_remark  VARCHAR(512) NOT NULL DEFAULT '' COMMENT '运维备注',

    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (id),
    UNIQUE KEY uk_order_no (order_no),
    UNIQUE KEY uk_order_token (order_token),
    UNIQUE KEY uk_app_idem (app_id, idempotency_key),
    UNIQUE KEY uk_app_client_order (app_id, client_order_no),
    KEY idx_app_user_status (app_id, user_id, status),
    KEY idx_trade_no (trade_no),
    KEY idx_status_expire (status, expire_at),
    KEY idx_closing_scan (status, updated_at, id),
    KEY idx_paid_at (paid_at),
    CONSTRAINT chk_amount CHECK (original_price - discount_amount = pay_amount),
    CONSTRAINT chk_amount_nonneg CHECK (original_price >= 0 AND discount_amount >= 0 AND pay_amount > 0 AND refunded_amount >= 0),
    CONSTRAINT chk_refund_le_pay CHECK (refunded_amount <= pay_amount)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单主表';


-- ============================================================
-- from: 20260716000002_create_order_logs.sql
-- ============================================================
-- order_logs:订单状态流转日志,每次状态变更追加一行(供时间线查询与审计)。
CREATE TABLE order_logs (
    id          BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    order_id    BIGINT UNSIGNED NOT NULL COMMENT '订单ID',
    order_no    VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '订单号',
    from_status TINYINT NOT NULL COMMENT '变更前状态',
    to_status   TINYINT NOT NULL COMMENT '变更后状态',
    operator    VARCHAR(64) NOT NULL DEFAULT 'system' COMMENT '操作人 system/user/admin:用户名',
    remark      VARCHAR(512) NOT NULL DEFAULT '' COMMENT '备注',
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    KEY idx_order_no (order_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单状态流转日志';


-- ============================================================
-- from: 20260716000003_create_event_outbox.sql
-- ============================================================
--
-- event_outbox(v3):事务性发件箱。租约 + 抖动退避 + 死信重放。
--
-- Outbox 只保证"状态变更与事件记录不因本地双写而断裂";投递是 at-least-once,必然
-- 可能重复(接入方按 event_id + aggregate_version 幂等/防乱序)。多副本 delivery worker
-- 用 (locked_by, locked_until) 租约或 SELECT ... FOR UPDATE SKIP LOCKED 抢占,互不重复投递;
-- 死信(status=2)不自动送达,由死信重放接口人工/定时重投。
--
-- collation:身份/路由列 ascii_bin,与 orders/order_events 的同名列一致(防跨表比较隐式转换)。
CREATE TABLE event_outbox (
    id           BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    event_id     VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '事件ID(UUID),幂等键',
    app_id       VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '投递目标接入方',
    order_no     VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '关联订单号',
    event_type   VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'order.paid/refunded/closed',
    aggregate_version BIGINT NOT NULL DEFAULT 0 COMMENT '事件聚合版本(排查/游标)',
    payload      VARBINARY(8192) NOT NULL COMMENT 'EventEnvelope(protobuf),webhook body 原文',

    status          TINYINT  NOT NULL DEFAULT 0 COMMENT '0待投递 1已投递 2死信',
    attempts        INT      NOT NULL DEFAULT 0 COMMENT '已尝试次数',
    next_attempt_at DATETIME NOT NULL COMMENT '下次投递时间(退避+抖动)',
    locked_by       VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '本次认领随机标识(每次重新生成,失败写回CAS)',
    locked_until    DATETIME NULL COMMENT '租约到期时间',
    last_error      TEXT     NULL COMMENT '最近失败原因',

    created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_event_id (event_id),
    KEY idx_claimable (status, next_attempt_at),
    KEY idx_order (order_no, aggregate_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='事件发件箱';


-- ============================================================
-- from: 20260716000004_create_app_endpoints.sql
-- ============================================================
--
-- app_endpoints(v3):接入方回调配置 + webhook 签名密钥引用(支持轮换)。
--
-- 签名 = HMAC-SHA256(timestamp + "." + raw_body, secret),头带 X-Order-Key-Id + X-Order-Timestamp;
-- 接入方按 key_id 选密钥、校验时间窗口(防重放)。轮换期间同时接受 current 与 prev 两把密钥。
--
-- 密钥不落库(评审补充):hmac_secret 明文入库泄露面过大,改存 *_ref —— 指向密钥管理系统
-- (KMS/Vault)的引用,投递前由 SecretResolver 按引用取明文,与 app_pay_accounts.config_ref 同模式。
-- webhook_url 需防 SSRF:仅 https、禁重定向、禁私网/元数据地址、限响应体、走出口白名单(应用层强制)。
CREATE TABLE app_endpoints (
    app_id              VARCHAR(32)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '接入方标识',
    webhook_url         VARCHAR(512) NOT NULL COMMENT '事件回调地址(https,禁私网)',
    key_id              VARCHAR(32)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '当前签名密钥ID',
    hmac_secret_ref     VARCHAR(255) NOT NULL COMMENT '当前签名密钥的密钥系统引用(不存明文)',
    prev_key_id         VARCHAR(32)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '轮换过渡:上一把密钥ID',
    prev_hmac_secret_ref VARCHAR(255) NOT NULL DEFAULT '' COMMENT '轮换过渡:上一把密钥引用',
    max_concurrency     INT          NOT NULL DEFAULT 4 COMMENT '单租户投递并发上限',
    enabled             TINYINT      NOT NULL DEFAULT 1 COMMENT '是否启用',
    created_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (app_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='接入方回调配置';


-- ============================================================
-- from: 20260716000005_create_refunds.sql
-- ============================================================
--
-- refunds(v4):退款执行流水 + 并发闭环约束。V1 一单一退(全额);审批在 admin。
--
-- 并发防护(见 DESIGN §10):RefundOrder 先 CAS 订单 PAID→REFUNDING 并与建退款记录同事务;
-- 活跃退款唯一(active_guard)保证一订单同时最多一笔活跃退款,失败/关闭记录保留可重提;
-- UNIQUE(app_id, out_refund_no) 幂等;request_hash 绑定退款请求(同退款号不同金额/原因/订单 → 冲突)。
--
-- 活跃守卫(评审 #7 修正):活跃集合含 ABNORMAL。异常退款(网关结果与订单冲突/金额账号不符)
-- 未经人工确认前必须阻断再次发起全额退款,否则可能重复退款。故活跃 = 0已建/1处理中/5异常;
-- 3失败/4关闭为可重提终态,不阻断。
--
-- pay_account_id:退款必须用【成功交易所属账号】(取订单 settled_pay_account_id,非下单首账号,
-- 见评审 #6)。worker 字段 next_query_at/query_count/locked_* 支持退款查单退避与多副本租约。
CREATE TABLE refunds (
    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    app_id         VARCHAR(32)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '接入方标识',
    refund_no      VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '本服务退款单号',
    out_refund_no  VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '调用方幂等键',
    request_hash   CHAR(64)     CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '退款请求哈希(out_refund_no+order+amount+reason)',
    order_no       VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '原订单号(按异常流水退款时取流水的 order_no,可为空串)',
    source_txn_id  BIGINT UNSIGNED NULL COMMENT '按异常流水退款的源支付流水 payment_transactions.id(NULL=订单级退款)',
    pay_account_id VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '成功交易账号(退款用,取订单 settled 账号;异常流水退款取流水账号)',

    refund_amount     BIGINT NOT NULL COMMENT '退款金额(分),V1=全额',
    status            TINYINT NOT NULL DEFAULT 0 COMMENT '0已建 1处理中 2成功 3失败 4关闭 5异常',
    gateway_refund_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '渠道退款单号',
    not_exist_count INT NOT NULL DEFAULT 0 COMMENT '连续明确退款不存在次数,非不存在结果或查询错误清零,不用于退避',
    gateway_confirmed TINYINT NOT NULL DEFAULT 0 COMMENT '渠道已受理或查到存在(历史在途保守保护),0否1是,确认后不可清零',
    reason            VARCHAR(512) NOT NULL DEFAULT '' COMMENT '退款原因',
    succeeded_at      DATETIME(3) NULL COMMENT '退款成功时间',
    last_error        TEXT NULL COMMENT '最近错误',

    -- worker 调度(退款查单退避 + 租约)。
    next_query_at  DATETIME NULL COMMENT '下次查单时间(PROCESSING 退避)',
    query_count    INT      NOT NULL DEFAULT 0 COMMENT '已查单次数',
    locked_by      VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '本次认领随机token,每次重新生成,非成功查询与重排写回须匹配',
    locked_until   DATETIME NULL COMMENT '租约到期',

    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    -- 活跃退款守卫:活跃(0已建/1处理中/5异常)时取来源键,否则 NULL;唯一索引保证一订单(订单级退款)
    -- 或一条源流水(按异常流水退款,键 'txn:<id>')最多一笔活跃退款,两类来源互不阻塞。
    active_guard   VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin
                     GENERATED ALWAYS AS (CASE WHEN status IN (0, 1, 5)
                       THEN IF(source_txn_id IS NULL, order_no, CONCAT('txn:', source_txn_id)) ELSE NULL END) STORED,
    PRIMARY KEY (id),
    UNIQUE KEY uk_refund_no (refund_no),
    UNIQUE KEY uk_app_out_refund (app_id, out_refund_no),
    UNIQUE KEY uk_active_refund (active_guard),
    KEY idx_order (order_no),
    KEY idx_source_txn (source_txn_id),
    KEY idx_status (status),
    KEY idx_query_due (status, next_query_at),
    CONSTRAINT chk_refund_amount_pos CHECK (refund_amount > 0),
    CONSTRAINT chk_refund_not_exist_count CHECK (not_exist_count >= 0),
    CONSTRAINT chk_refund_gateway_confirmed CHECK (gateway_confirmed IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='退款执行流水';


-- ============================================================
-- from: 20260716000006_create_payment_attempts.sql
-- ============================================================
--
-- payment_attempts(v4):支付尝试状态机。解决 CreateOrder→网关下单的"结果未知"窗口。
--
-- 状态:0已建 1请求中 2就绪(可调起) 3结果未知 4已支付 5关单中 6已关闭 7失败。
-- 规则(见 DESIGN §8/§9):一订单可多次尝试,但同一时刻只允许一个活跃尝试;
-- 换支付方式=开新尝试(旧的关单)。out_trade_no 归尝试(每次独立,对账可区分)。
--
-- 活跃唯一(评审 #5 修正):活跃集合含 CLOSING。若只把 0-3 视为活跃,则旧尝试进入
-- CLOSING(已向网关发起关单、结果未知)时唯一守卫失效,可并发建新 attempt → 双支付风险。
-- 故活跃 = 0,1,2,3,5(排除 4已付/6已关/7失败三个终态)。
--
-- worker 调度字段:next_query_at/query_count 支持 READY/UNKNOWN/CLOSING 的退避查单;
-- locked_by 每次认领保存新随机 token,写回匹配 token;locked_until 为多副本租约截止时间。
CREATE TABLE payment_attempts (
    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    app_id         VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '接入方标识',
    order_no       VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '订单号',
    attempt_no     INT          NOT NULL COMMENT '第几次尝试(从 1 起)',
    channel        VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'wechat/alipay',
    pay_account_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '本次尝试使用的支付账号',
    config_version INT          NOT NULL DEFAULT 0 COMMENT '支付账号配置版本(冻结)',
    out_trade_no   VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '提交给网关的商户订单号(每尝试独立)',
    request_hash   CHAR(64)    CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '本次下单请求哈希',
    status         TINYINT      NOT NULL DEFAULT 0 COMMENT '0建 1请求中 2就绪 3未知 4已付 5关单中 6已关 7失败',
    prepay_id      VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '网关预支付ID(复用调起)',
    last_error     TEXT         NULL COMMENT '最近错误',
    expire_at      DATETIME     NOT NULL COMMENT '本次尝试的支付截止',

    -- worker 调度(查单退避 + 租约)。
    next_query_at  DATETIME     NULL COMMENT '下次查单时间(READY/UNKNOWN/CLOSING 退避,空值立即可查)',
    query_count    INT          NOT NULL DEFAULT 0 COMMENT '已查单次数',
    locked_by      VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '本次认领随机token,每次重新生成,非成功查询与重排写回须匹配',
    locked_until   DATETIME     NULL COMMENT '租约到期',

    -- 活跃唯一:活跃(0,1,2,3,5)时取 order_no,否则 NULL;唯一索引保证一订单最多一个活跃尝试。
    active_order   VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin
                     GENERATED ALWAYS AS (CASE WHEN status IN (0,1,2,3,5) THEN order_no ELSE NULL END) STORED,
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_order_attempt (order_no, attempt_no),
    UNIQUE KEY uk_out_trade_no (out_trade_no),
    UNIQUE KEY uk_active_order (active_order),
    KEY idx_status (status),
    KEY idx_query_due (status, next_query_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支付尝试状态机';


-- ============================================================
-- from: 20260716000007_create_app_pay_accounts.sql
-- ============================================================
--
-- app_pay_accounts:多租户支付账号(评审要求)。go-pay 的 paymgr.Manager 内部按 Channel
-- 只存一个 Provider,同渠道重复注册会覆盖——因此多个项目用不同微信商户号时,必须为
-- 每个账号建独立 Provider/Manager,由 PaymentGatewayRouter 按 (pay_account_id, channel) 路由。
--
-- 证书/私钥等敏感材料不落此表:config_ref 指向密钥管理系统的引用,config_version 用于轮换。
-- 订单下单时冻结 pay_account_id + mch_id + app_id + config_version,退款/查单/关单沿用原账号。
-- collation:身份/路由列 ascii_bin,notify_route_id 必须精确匹配(回调路由)。
CREATE TABLE app_pay_accounts (
    reconcile_scan_version SMALLINT NOT NULL DEFAULT 1 COMMENT '历史建批次游标算法版本;升级后从created_at重扫',
    reconcile_local_seed_cursor BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '历史本地流水回填ID;新写入同事务队列覆盖晚提交',

    pay_account_id  VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '支付账号ID(路由键)',
    app_id          VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '归属接入方',
    channel         VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'wechat/alipay',
    mch_id          VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '微信商户号或支付宝收款seller_id,启用账号必填,无空值回落;用于回调归属校验',
    pay_app_id      VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '微信/支付宝应用ID',
    notify_route_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '回调路由标识(不透明,URL /notify/{ch}/{route_id} 用;非安全凭证)',
    config_ref      VARCHAR(255) NOT NULL COMMENT '密钥管理系统中的配置引用(证书/私钥)',
    config_version  INT         NOT NULL DEFAULT 1 COMMENT '配置版本(轮换)',
    enabled         TINYINT     NOT NULL DEFAULT 1 COMMENT '是否启用',
    reconcile_scanned_through DATE NULL DEFAULT NULL COMMENT '已确保连续账期批次存在的游标;NULL从created_at日起步,不是完成水位',
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (pay_account_id),
    UNIQUE KEY uk_notify_route (notify_route_id),
    KEY idx_app_channel (app_id, channel)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='多租户支付账号';


-- ============================================================
-- from: 20260716000008_create_payment_transactions.sql
-- ============================================================
--
-- payment_transactions(v4):不可变/追加式财务流水。支付尝试 ≠ 财务流水。
--
-- 每笔"网关确认的成功支付/退款"各追加一条,只插入不更新。用于渠道账单对账、
-- 重复交易识别(transaction_id 唯一)、审计——不依赖可变的订单主表。
--
-- 资金真相优先(评审 #4):网关确认的资金事实必须落此表,即使订单 CAS 未胜出。
--   · 订单已被另一交易支付时:仍提交这条流水,anomaly=1(第二笔真实付款),不再改订单/不再发 order.paid;
--   · 金额或账号与订单快照不一致的真实交易:落 anomaly=1 后告警,不静默回滚丢弃。
-- refund_no(评审 #7):退款流水(kind=2)直接回指 refunds.refund_no,便于财务对账串联。
CREATE TABLE payment_transactions (
    id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    app_id             VARCHAR(32)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '接入方标识',
    order_no           VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '订单号',
    payment_attempt_id BIGINT UNSIGNED NULL COMMENT '关联支付尝试(退款可空)',
    refund_no          VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '关联退款单号(kind=2 退款流水回指 refunds.refund_no)',
    pay_account_id     VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '支付账号',
    channel            VARCHAR(32)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'wechat/alipay',
    kind               TINYINT      NOT NULL COMMENT '1支付 2退款',
    out_trade_no       VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '商户订单/退款号',
    transaction_id     VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '资金流水唯一标识(支付为渠道交易号,支付宝退款为refund:请求号)',
    amount             BIGINT       NOT NULL COMMENT '金额(分)',
    currency           VARCHAR(8)   CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'CNY' COMMENT '币种',
    anomaly            TINYINT      NOT NULL DEFAULT 0 COMMENT '0正常 1资金异常(第二笔真实付款/金额账号不符,待人工)',
    anomaly_kind       VARCHAR(32)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '异常成因(second_real_payment/amount_mismatch/account_channel_mismatch;空=成因未落库的历史行)',
    paid_at            DATETIME(3)  NULL COMMENT '发生时间',
    notify_digest      VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '通知来源标记(alipay-notify/wechat-notify/query)',
    created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入账时间',
    PRIMARY KEY (id),
    -- 不同渠道 transaction_id 命名空间不同,唯一键含 channel + 账号;
    -- 重复回调命中唯一键时必须重读原流水校验订单/金额/账号一致,不一致记高等级资金异常。
    UNIQUE KEY uk_txn (channel, pay_account_id, transaction_id),
    KEY idx_order (order_no),
    KEY idx_refund (refund_no),
    KEY idx_account_time (pay_account_id, paid_at),
    KEY idx_account_seed (channel, pay_account_id, id),
    KEY idx_anomaly (anomaly),
    CONSTRAINT chk_txn_amount_pos CHECK (amount > 0),
    CONSTRAINT chk_txn_kind CHECK (kind IN (1, 2))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='财务流水(不可变)';


-- ============================================================
-- from: 20260716000009_create_order_events.sql
-- ============================================================
--
-- order_events(v4):不可变事件历史,ListOrderEvents 游标查询的权威数据源。
--
-- 与 event_outbox 分离:outbox 是可重试/死信/清理的投递任务;order_events 是长期保留的
-- 不可变历史。写事件时同事务写两处(outbox 投递 + order_events 历史)。
-- 自增 id 作单调游标(不用 UUID event_id 排序);(app_id, id) 支持按接入方游标拉取补偿漏投。
--
-- 语义唯一(评审补充):UNIQUE(app_id, order_no, aggregate_version) 保证"同一次状态跃迁"
-- 只产生一条事件;否则并发/重试可能对同一版本生成两个不同 event_id,接入方按 event_id 去重
-- 也拦不住,导致重复发权益。
CREATE TABLE order_events (
    id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键,单调游标',
    event_id          VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '事件ID(UUID)',
    app_id            VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '接入方',
    order_no          VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '订单号',
    event_type        VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'order.paid/refunded/closed',
    aggregate_version BIGINT NOT NULL COMMENT '聚合版本',
    payload           VARBINARY(8192) NOT NULL COMMENT 'EventEnvelope(protobuf),完整状态快照',
    created_at        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '产生时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_event_id (event_id),
    UNIQUE KEY uk_order_version (app_id, order_no, aggregate_version),
    KEY idx_app_cursor (app_id, id),
    KEY idx_order (order_no, aggregate_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='事件历史(不可变,游标查询源)';


-- ============================================================
-- from: 20260716000010_create_reconcile_batches.sql
-- ============================================================
--
-- reconcile_batches(v1 上线必需,评审 #10):支付渠道账单对账批次。
--
-- 每日 CLI/worker 按 (channel, pay_account_id, bill_date) 拉取渠道账单为一个批次,
-- 与本地 payment_transactions 逐笔比对,差异写 reconcile_diffs。批次唯一防重(同日同账号
-- 只对账一次),支持多副本租约抢占(locked_by/locked_until)。
CREATE TABLE reconcile_batches (
    algorithm_version SMALLINT NOT NULL DEFAULT 1 COMMENT '内部算法代次;1历史支付,2支付与退款',
    run_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '当前比较代次',
    source_run_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '当前完整源导入代次,重比沿用',
    published_run_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '对外可见完整比较代次',
    published_source_run_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '对外可见完整账单源代次',
    import_complete BOOLEAN NOT NULL DEFAULT FALSE COMMENT '完整性与汇总校验成功后封存',
    compare_after_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '源证据逐页比较检查点',
    dirty_seq BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '迟到本地资金触发的重比序号',
    run_dirty_seq BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当前比较已接收的重比序号',
    published_dirty_seq BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '已发布比较覆盖的重比序号',
    source_digest_algorithm VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '源文件审计摘要算法,旧记录留空',
    source_digest VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '源文件摘要,仅审计不替代渠道成功复核',
    source_refund_amount BIGINT NOT NULL DEFAULT 0 COMMENT '渠道退款本金合计,分',
    local_refund_amount BIGINT NOT NULL DEFAULT 0 COMMENT '匹配范围内本地退款本金合计,分',

    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    batch_no       VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '对账批次号(本服务生成,唯一)',
    app_id         VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '接入方(账号可跨 app 时留空)',
    channel        VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'wechat/alipay',
    pay_account_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '支付账号',
    bill_date      DATE        NOT NULL COMMENT '账单日期',

    status         TINYINT NOT NULL DEFAULT 0 COMMENT '0待拉取 1已拉取 2对账完成 3失败',
    source_count   INT     NOT NULL DEFAULT 0 COMMENT '渠道账单条数',
    matched_count  INT     NOT NULL DEFAULT 0 COMMENT '匹配一致条数',
    diff_count     INT     NOT NULL DEFAULT 0 COMMENT '差异条数',
    source_amount  BIGINT  NOT NULL DEFAULT 0 COMMENT '渠道账单总额(分)',
    local_amount   BIGINT  NOT NULL DEFAULT 0 COMMENT '本地流水总额(分)',

    locked_by      VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '本次认领随机标识(每次重新生成,完成与失败写回CAS)',
    locked_until   DATETIME NULL COMMENT '执行租约到期;失败且无持有者时为下次可重试时间',
    pulled_at      DATETIME NULL COMMENT '账单拉取完成时间',
    reconciled_at  DATETIME NULL COMMENT '对账完成时间',
    last_error     TEXT NULL COMMENT '最近错误',

    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_batch_no (batch_no),
    UNIQUE KEY uk_account_date (channel, pay_account_id, bill_date, algorithm_version),
    KEY idx_status (status, bill_date),
    KEY idx_reconcile_claim (channel, pay_account_id, algorithm_version, status, locked_until, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='渠道账单对账批次';


-- ============================================================
-- from: 20260716000011_create_reconcile_diffs.sql
-- ============================================================
--
-- reconcile_diffs(v1 上线必需,评审 #10):对账差异 + 人工处理审计。
--
-- 差异类型:长款(网关有本地无)、短款(本地有网关无)、金额不一致、状态不一致。
-- 每条差异带独立处理状态与人工处理审计(处理人/时间/备注),不能只有清单无处理入口。
-- 自动长短款处理为 roadmap;V1 落库 + 告警 + 人工处理入口。
CREATE TABLE reconcile_diffs (
    run_id VARCHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '比较代次;空为旧算法或本地单边差异',
    entry_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '源证据ID;0禁止自动资金补偿',
    kind TINYINT NOT NULL DEFAULT 1 COMMENT '1支付 2退款',
    identity_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '完整资金身份SHA256;本地单边按流水ID',
    local_txn_id BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '本地不可变资金流水ID',
    original_transaction_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '退款原支付渠道交易号',
    out_refund_no VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '商户退款请求号',
    refund_no VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '本地退款单号',
    currency VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '核对币种;旧差异未知留空',
    verify_attempts INT NOT NULL DEFAULT 0 COMMENT '正向核实累计次数,不因重试清零',
    next_verify_at DATETIME(3) NULL DEFAULT NULL COMMENT '正向补偿下次可尝试时间;NULL首次可领取',
    verify_locked_by VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '每次补偿认领随机token',
    verify_locked_until DATETIME(3) NULL DEFAULT NULL COMMENT '补偿租约截止,过期结果禁止写回',
    verify_last_error TEXT NULL COMMENT '脱敏补偿结果,不写下载URL或原始响应',

    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    batch_id       BIGINT UNSIGNED NOT NULL COMMENT '所属对账批次',
    app_id         VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '接入方',
    channel        VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'wechat/alipay',
    pay_account_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '支付账号',

    diff_type      TINYINT NOT NULL COMMENT '1长款(网关有本地无) 2短款(本地有网关无) 3金额不一致 4状态不一致',
    order_no       VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '订单号(长款可空)',
    transaction_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '第三方交易号',
    out_trade_no   VARCHAR(64)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' COMMENT '商户订单号',
    gateway_amount BIGINT NOT NULL DEFAULT 0 COMMENT '渠道金额(分)',
    local_amount   BIGINT NOT NULL DEFAULT 0 COMMENT '本地金额(分)',
    detail         VARCHAR(1024) NOT NULL DEFAULT '' COMMENT '差异明细(渠道/本地字段快照)',

    handle_status  TINYINT NOT NULL DEFAULT 0 COMMENT '0待处理 1处理中 2已解决 3已忽略',
    handled_by     VARCHAR(64) NOT NULL DEFAULT '' COMMENT '处理人(人工审计)',
    handled_at     DATETIME NULL COMMENT '处理时间',
    handle_remark  VARCHAR(512) NOT NULL DEFAULT '' COMMENT '处理备注(审计)',

    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    -- 同批次内一笔交易只记一条差异,重跑对账幂等。
    UNIQUE KEY uk_batch_txn (batch_id, run_id, kind, identity_key, diff_type),
    KEY idx_repair_claim (channel, pay_account_id, handle_status, next_verify_at, verify_locked_until, id),
    KEY idx_local_diff (batch_id, local_txn_id),
    KEY idx_handle (handle_status, diff_type),
    KEY idx_order (order_no),
    CONSTRAINT chk_diff_type CHECK (diff_type IN (1, 2, 3, 4))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对账差异(人工处理审计)';


-- ============================================================
-- from: 20260818000001_create_ops_audit_logs.sql
-- ============================================================
--
-- ops_audit_logs:运维动作审计,【只插不改】。覆盖死信人工/自动重放、对账差异处置;
-- 写入与被审计动作同一事务。不写在 event_outbox 上(投递任务可清理,审计不可随之丢失)。
CREATE TABLE ops_audit_logs (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    actor       VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '动作发起者(app_id:user_id,自动动作为 system)',
    action      VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '动作标识(dead_letter_requeue/reconcile_diff_handle)',
    target_type VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '被操作对象类型(event_outbox/reconcile_diff)',
    target_id   VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '被操作对象标识(event_id/差异id)',
    detail      VARCHAR(1024) NOT NULL DEFAULT '' COMMENT '动作细节(状态前后值/重放前尝试次数等,可复盘)',
    created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '写入时间(服务端)',
    PRIMARY KEY (id),
    KEY idx_target (target_type, target_id),
    KEY idx_action_time (action, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='运维动作审计(只插不改)';

-- ============================================================
-- from: 20260819000001_create_app_amount_policies.sql
-- ============================================================
--
-- per-app 金额策略(实付上下限/业务类型白名单/币种白名单)。只拦异常值,金额真实性
-- 由 PricingAuthority 边界保证。未配置或 enabled=0 的 app 回退全局默认档(应用层回退链)。
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


-- 分页账单证据和本地资金唤醒（000003）
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
