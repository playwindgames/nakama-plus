-- 调度器集成层用例所需的 leaderboard 表。
--
-- `invokeCallback` 里 tournament 的两个分支跑裸 SQL（`ls.db` 是 `*sql.DB` 具体类型、
-- 无法 mock），只能上真库。这张表就是它们读写的对象。
--
-- 结构取自生产同构的 `SHOW CREATE TABLE leaderboard`（2026-08-20 采集），
-- 只保留那两段 SQL 会碰的列与主键 —— 索引与 CHECK 约束对测试无影响，故略去。
--
--   读：id, sort_order, operator, reset_schedule, metadata, create_time,
--       category, description, duration, end_time, max_size, max_num_score,
--       title, size, start_time
--   写：UPDATE leaderboard SET size = 0
--
-- ⚠️ **刻意写成 Postgres 与 CockroachDB 都能吃的方言**，因为它有两个使用者：
--   本地  `tools/setup-test-db.sh` → 复现集群的 CockroachDB
--   门禁  gate workflow 的集成层 job → postgres:16.8-alpine（与上游
--         `docker-compose-tests.yml` 同版本）
-- 两边各写一份 DDL 必然漂移，而漂移的后果是「门禁里跑的表结构不是本地那张」。
-- 因此这里避开了两处方言：CockroachDB 的 `CREATE DATABASE IF NOT EXISTS`
-- 与主键里的 `(id ASC)`，后者 Postgres 不接受。
--
-- ⚠️ 这**不是**真实的迁移产物。高保真那一路走的是上游 `docker-compose-tests.yml`
-- （`nakama migrate up` 建真表），它在 PR 上由 `tests.yaml` 跑。
-- 本文件服务的是「直推 main 也要跑、且要快」的那条路径。

CREATE TABLE IF NOT EXISTS leaderboard (
  id             VARCHAR(128) NOT NULL,
  authoritative  BOOL         NOT NULL DEFAULT false,
  sort_order     INT2         NOT NULL DEFAULT 1,
  operator       INT2         NOT NULL DEFAULT 0,
  reset_schedule VARCHAR(64)  NULL,
  metadata       JSONB        NOT NULL DEFAULT '{}',
  create_time    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  category       INT2         NOT NULL DEFAULT 0,
  description    VARCHAR(255) NOT NULL DEFAULT '',
  duration       INT8         NOT NULL DEFAULT 0,
  end_time       TIMESTAMPTZ  NOT NULL DEFAULT '1970-01-01 00:00:00+00:00',
  join_required  BOOL         NOT NULL DEFAULT false,
  max_size       INT8         NOT NULL DEFAULT 100000000,
  max_num_score  INT8         NOT NULL DEFAULT 1000000,
  title          VARCHAR(255) NOT NULL DEFAULT '',
  size           INT8         NOT NULL DEFAULT 0,
  start_time     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  enable_ranks   BOOL         NULL DEFAULT true,
  CONSTRAINT leaderboard_pkey PRIMARY KEY (id)
);
