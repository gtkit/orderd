-- 增量草稿(人工评审执行;生产禁 AutoMigrate):新建 ops_audit_logs。
--
-- ops_audit_logs:运维动作审计,【只插不改】——应用只暴露插入与查询,不提供更新/删除。
-- 覆盖动作:死信人工重放、死信自动兜底重放(actor=system)、对账差异处置。
-- 写入与被审计动作在同一事务内(动作回滚则审计不存在,审计失败则动作回滚)。
-- 不写在 event_outbox 上:outbox 是可清理的投递任务,审计会随清理丢失(DESIGN §12/§13)。
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
