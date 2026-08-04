# `.github` CI/CD 与部署 Wiki

本文档只描述当前仓库的 CI/CD 与容器部署边界。代码、workflow、Dockerfile、Compose、入口脚本和测试不一致时，以实际实现和测试为准；修改其中任一项后，必须重新核对本文档及根目录 `AGENTS.md`。

## 1. GHCR 发布 workflow

唯一的工作流是 [`workflows/publish-ghcr.yml`](workflows/publish-ghcr.yml)，名称为 `Publish Docker image to GHCR`。它负责构建和发布镜像，不负责部署运行环境。

### 触发、权限和发布条件

| 事件 | 当前行为 |
|---|---|
| `push` 到 `main` 或 `master` | 构建并推送镜像 |
| `push` `v*` tag | 构建并推送镜像；只有符合语义版本格式的 tag 才会生成 semver 标签 |
| `pull_request` | 构建校验，不登录 GHCR，不推送镜像 |
| `workflow_dispatch` | 手工运行构建并推送流程 |

Job 在 `ubuntu-latest` 运行，权限为读取仓库内容和写入 Packages。步骤依次使用 checkout、QEMU、Buildx、GHCR 登录、Docker metadata 和 build-push actions。GHCR 登录条件是事件不是 pull request；build-push 的 `push` 条件与此相同。

镜像名由 `GITHUB_REPOSITORY_OWNER` 和仓库名转为小写后组成：

```text
ghcr.io/<owner-lowercase>/<repository-lowercase>
```

### 标签和构建矩阵

metadata-action 当前配置生成：

- 分支引用标签（`type=ref,event=branch`）;
- pull request 引用标签（`type=ref,event=pr`，仅用于 PR 构建）;
- 语义版本 tag 的完整版本和 `major.minor` 标签;
- Git SHA 标签;
- 只有 GitHub 仓库的默认分支才额外生成 `latest`。

构建上下文是仓库根目录，Dockerfile 为根目录 [`Dockerfile`](../Dockerfile)，平台为 `linux/amd64` 和 `linux/arm64`。QEMU 用于跨架构构建，Buildx 使用 GitHub Actions cache。该工作流没有 pytest、ruff、迁移验证、漏洞扫描、镜像签名/证明或自动部署步骤，因此“镜像构建成功”不等于测试通过、部署成功或交易系统验收通过。

### 镜像内容约束

当前 [`Dockerfile`](../Dockerfile) 的镜像契约是：

- 基于 `python:3.12-slim`，设置非缓冲 Python、`uv` 运行路径，并安装构建工具和 curl；
- 先复制 `pyproject.toml`、`uv.lock` 等依赖文件，优先执行 `uv sync --frozen`，失败后回退到非 frozen 安装；复制源码和迁移文件后再次同步项目；
- 复制 `src`、`alembic.ini`、`migrations` 和 [`docker-entrypoint.sh`](../docker-entrypoint.sh)，创建 `/app/reports` 与 `/app/.vntrader`；
- 入口脚本先于默认命令运行，默认命令为 `vntdr live`。

由于 frozen 安装失败会自动回退，构建成功本身不能证明最终依赖完全按锁文件安装；发布前应调查 frozen 失败原因。镜像没有显式声明非 root `USER`，生产部署的容器权限、主机隔离和镜像来源仍需单独评估。

## 2. Compose 拓扑和运行契约

[`docker-compose.yml`](../docker-compose.yml) 定义 `db`、`cache`、`quant_core`、`etf_ingest`、`webapp` 五个服务，全部加入 `quant_net` bridge 网络；服务默认 `restart: always`。三个应用服务当前都使用可变引用 `ghcr.io/simonsmh/vntdr:latest`，所以不能把 Compose 默认配置当作可复现的生产版本锁定。

| 服务 | 启动依赖与健康检查 | 持久化/入口 |
|---|---|---|
| `db` | `postgres:18`；`pg_isready`，间隔 10 秒、超时 5 秒、重试 10 次 | `pg_data:/var/lib/postgresql`；注入 `PG_*` |
| `cache` | `redis:latest`；`redis-cli ping`，间隔 10 秒、超时 5 秒、重试 10 次 | `redis_data:/data`；按环境变量设置 AOF、save 和淘汰策略 |
| `quant_core` | 等待 `db`、`cache` healthy；`vntdr doctor`，间隔 30 秒、超时 10 秒、重试 3 次 | `vntdr live`；`vntrader_data:/app/.vntrader`、`reports_data:/app/reports`、`config_data:/root/.vntdr` |
| `etf_ingest` | 等待 `db` healthy 和 `quant_core` healthy；自身无 healthcheck | `vntdr etf-flow-scheduler`；共享 `reports_data:/app/reports` |
| `webapp` | 等待 `db`、`cache` healthy；自身无 healthcheck | `vntdr gradio --port <container-port>`；共享 `config_data:/root/.vntdr` |

`webapp` 的端口映射为宿主 `GRADIO_PORT` 到容器 `GRADIO_CONTAINER_PORT`，默认都是 7860。命名卷完整列表为 `pg_data`、`redis_data`、`vntrader_data`、`reports_data` 和 `config_data`：数据库事实、Redis 运行状态、VeighNa 数据、报告和配置覆盖彼此分离；应用镜像回滚不会回滚这些卷。

Compose 本身没有应用级自动回滚，也没有 nginx、TLS、域名、认证或防火墙配置。健康状态只反映对应检查通过，不能证明有监控目标、持续产生信号或正在交易。

## 3. 迁移与配置注入

### 入口脚本和迁移

[`docker-entrypoint.sh`](../docker-entrypoint.sh) 使用 `set -eu`。只有 `VNTDR_RUN_MIGRATIONS` 的值严格为 `true` 时才会：

1. 通过 `Settings.from_env()` 计算数据库 DSN；
2. 执行 `alembic upgrade head`；
3. 成功后 `exec` 容器命令。

当前 Compose 只给 `quant_core` 注入 `VNTDR_RUN_MIGRATIONS=true`；`etf_ingest` 和 `webapp` 明确为 `false`。因此 `quant_core` 是启动迁移入口，迁移失败会在执行 `vntdr live` 前退出；配合 `restart: always` 可能表现为反复重启。脚本没有自动 downgrade 或回滚数据库 schema 的逻辑。

### 环境和共享配置

`stack.env.example` 只是模板，Compose 不会自动把它当作真实配置使用。部署者应通过 `--env-file stack.env` 或受控环境注入值；真实 `stack.env` 不得提交。Compose 当前显式注入的配置包括：

- OKX REST 地址、demo 开关和三项 API 凭据变量；
- PostgreSQL 主机、端口、用户名、密码变量和数据库名；
- Redis 主机、端口、DB、AOF、save 和淘汰策略；
- Telegram 变量；
- ETF 显式 watchlist、最小市值和 universe 上限；
- Gradio 宿主/容器端口。

模板当前将 OKX demo 设为安全默认值，凭据和 Telegram 值留空；ETF watchlist 留空时使用动态 universe，默认跟踪当前总市值至少 100 亿元的 ETF，必要时可用显式列表覆盖。示例中的数据库密码只是占位配置，生产必须替换并限制配置文件权限。

`quant_core` 和 `webapp` 通过 `config_data` 共享 `/root/.vntdr/config_override.json`。Gradio 设置和监控目标等持久化覆盖由 `ConfigService` 写入该卷，`quant_core` 后续运行会重新加载。`VNTDR_RUN_MIGRATIONS` 由 Compose 按服务硬编码，不来自模板；同样，`VNTDR_EXECUTION_MODE`、`VNTDR_DEFAULT_*` 等未列入当前 Compose `environment` 白名单的变量，仅写进 `stack.env` 并不会自动进入容器，必须由 Compose/部署覆盖显式传递，或使用受控的共享配置覆盖。配置覆盖卷可能包含敏感值，必须限制访问，不能把其内容打印到日志或提交到 Git。

## 4. 生产安全、版本和回滚

### 不可越过的安全边界

- 凭据只能通过受控运行环境或 secret manager 注入；不要写入 Git、Wiki、报告、截图、命令输出或普通日志。
- Compose 和模板默认使用 demo 模式；OKX 凭据不完整时，运行时组装 `SimulatedOrderExecutor`。凭据完整时虽可组装 `OkxOrderExecutor`，当前发布版本的监控流程仍明确抑制订单执行并记录 `orders intentionally suppressed`；`execution_mode=live` 不是已经开通实盘下单的证明。
- 开放真实执行前必须单独完成安全评审、OKX demo 验证、订单幂等/成交确认/故障恢复测试和发布审批。当前验收仍以通知、研究和影子运行边界为准。
- `webapp` 直接发布 Gradio 端口；仓库没有 nginx/TLS/域名/认证配置。生产必须在外部部署层提供访问控制、TLS 和反向代理，不能把这些能力记录为仓库已交付能力。
- `latest`、`redis:latest` 及未固定 digest 的基础镜像会漂移。生产应记录镜像 digest、构建 SHA、迁移 revision 和配置版本，并使用部署层的不可变版本引用；PR 标签不能作为生产版本。
- 部署前备份 PostgreSQL，并按需备份 `reports_data`、`config_data` 和其他运行状态。禁止在生产执行 `docker compose down -v`，因为它会删除命名卷及其数据。

### 回滚原则

1. 发布前记录当前镜像 digest、Git SHA、Alembic revision、共享配置覆盖和数据库备份；先在隔离环境验证候选镜像。
2. 当前 Compose 把应用镜像写死为 `latest`，没有 `VNTDR_IMAGE` 或 tag 参数。生产回滚必须由外部部署覆盖固定到已验证的版本 tag 或 digest，并保留数据库和配置卷；不能只重新拉取 `latest`。
3. 应用镜像回滚不会自动逆转已执行的 `alembic upgrade head`。若新 schema 与旧应用不兼容，不要盲目 downgrade 或删除卷，应按数据库恢复流程恢复备份，再部署兼容的应用版本。
4. 回滚后依次确认 `db`/`cache` healthy、`quant_core` 的 `vntdr doctor`、迁移日志、应用日志、共享配置和 Gradio 入口；最后再恢复 ETF 调度或其他依赖服务。

## 5. 排障顺序

以下命令不得把含敏感值的完整配置或 DSN 粘贴到工单、聊天或日志中：

```bash
docker compose --env-file stack.env ps
docker compose --env-file stack.env config --quiet
docker compose --env-file stack.env logs --tail=200 quant_core
docker compose --env-file stack.env logs --tail=200 db cache webapp etf_ingest
docker compose --env-file stack.env exec quant_core vntdr doctor
docker compose --env-file stack.env exec cache redis-cli ping
docker compose --env-file stack.env port webapp 7860
```

按以下顺序定位：

1. `ps` 先看服务是否反复重启、健康状态和端口；`config --quiet` 只做 Compose 插值校验，避免使用会回显全部环境值的普通 `config`。
2. `db`/`cache` unhealthy 时，先看对应日志、网络和宿主资源；`quant_core`、`webapp`、`etf_ingest` 的启动顺序依赖这些 healthcheck。
3. `quant_core` 反复重启时，优先检查数据库连接、迁移 revision、镜像架构/tag 和环境注入；入口迁移错误会阻止 `vntdr live` 启动。
4. `doctor` 通过但没有信号时，确认共享配置中有 `research.monitored_targets`，并检查历史数据、Redis 状态和关闭 K 线；进程健康不等于有监控目标。
5. 没有订单时，先确认当前通知-only/订单抑制边界，不要把 `MonitorResult.actions` 当作已成交订单；Telegram 未配置时只能依赖日志和状态面板。
6. `etf_ingest` 未启动时，检查 `quant_core` 是否 healthy、ETF 配置及外部 AkShare 限流/字段错误；单次采集失败不应被误判为数据库迁移问题。
7. GHCR 拉取失败时，核对镜像仓库可见性、不可变 tag/digest、平台架构和部署环境的包读取权限，不要在日志中暴露访问令牌。

## 6. 测试和文档同步

当前 workflow 没有测试门禁。部署相关的离线回归至少包括：

```bash
uv run pytest tests/unit/test_dockerfile.py tests/unit/test_notify_only.py \
  tests/integration/test_alembic_migration.py tests/integration/test_cli.py
```

这些测试分别覆盖镜像中依赖/源码复制顺序、通知-only 安全默认、Alembic 升级后的表结构，以及 `doctor`/CLI 运行边界。涉及真实 PostgreSQL、Redis、OKX、Telegram 或 GHCR 的验证必须显式准备隔离环境；不得在 CI 或本地测试中隐式使用生产凭据。修改 Compose/迁移/外部边界时，还应按 `tests/AGENTS.md`、`tests/unit/AGENTS.md` 和 `tests/integration/AGENTS.md` 选择分层测试。

任何 workflow、Dockerfile、Compose、入口脚本、依赖锁文件、镜像标签、部署环境变量、迁移或运行时安全边界的变更，都必须在同一提交检查并更新本文件、根目录 `AGENTS.md` 及受影响模块/迁移/测试目录的 `AGENTS.md`。新增 CI 检查必须记录触发条件、是否需要外部凭据、失败含义和是否会推送/部署；不得把镜像发布误写成完整 CI、生产部署或真实交易验收。
