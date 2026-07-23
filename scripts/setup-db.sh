#!/usr/bin/env bash
# 建 orderd 独立库并应用权威 schema(docs/sql/schema.sql)。幂等:库已有表则跳过。
# 连接参数走环境变量:
#   MYSQL_HOST(默认 127.0.0.1) MYSQL_PORT(3306) MYSQL_USER(root) MYSQL_PASSWORD(空) ORDERD_DB(orderd)
# 用法:  bash scripts/setup-db.sh   或   make setup-db
# 说明:本项目无迁移工具;schema.sql 为单一权威草稿,生产由 DBA/发布流程评审执行,禁 AutoMigrate。
set -euo pipefail

MHOST="${MYSQL_HOST:-127.0.0.1}"
MPORT="${MYSQL_PORT:-3306}"
MUSER="${MYSQL_USER:-root}"
MPASS="${MYSQL_PASSWORD:-}"
MDB="${ORDERD_DB:-orderd}"
SCHEMA="$(cd "$(dirname "$0")/.." && pwd)/docs/sql/schema.sql"

if ! command -v mysql >/dev/null 2>&1; then
  echo "错误:未找到 mysql 客户端" >&2
  exit 1
fi

my() {
  if [ -n "$MPASS" ]; then
    mysql -h"$MHOST" -P"$MPORT" -u"$MUSER" -p"$MPASS" "$@"
  else
    mysql -h"$MHOST" -P"$MPORT" -u"$MUSER" "$@"
  fi
}

my -e "CREATE DATABASE IF NOT EXISTS \`$MDB\` DEFAULT CHARSET=utf8mb4;"
n="$(my -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$MDB';")"
if [ "$n" -gt 0 ]; then
  echo "库 $MDB 已有 $n 张表,跳过迁移(如需重建请先手动 DROP 相关表)。"
  exit 0
fi

echo "应用 $SCHEMA"
my "$MDB" < "$SCHEMA"

echo "完成:$MDB 表数=$(my -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$MDB';")"
echo "提示:支付账号按部署自行播种,例如支付宝沙箱:"
echo "  INSERT INTO app_pay_accounts(pay_account_id,app_id,channel,mch_id,pay_app_id,notify_route_id,config_ref,config_version,enabled)"
echo "  VALUES('acc-alipay','app1','alipay','','<APPID>','alipay-sbx','env',1,1);"
