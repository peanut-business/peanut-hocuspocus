# peanut-hocuspocus

与 peanut-admin-code 同级的独立实时协作服务，使用锁定的 Hocuspocus 4.6.0 与 Yjs 13.6.32。完整服务源码位于 src，依赖解析固定在 pnpm-lock.yaml；不随目录调整升级依赖。

服务只通过受认证的 HTTPS 接口取得服务端核实的编辑者身份、连接授权、最新持久化 Yjs 草稿和实际保存回执。应用 PHP 负责统一身份、功能权限、文档数据权限、文件引用、幂等、状态比较与历史版本；服务不连接业务 SQL，不导入或挂载应用 PHP、前端源码。

原生房间先合并和广播，随后批量自动保存。首位加入者加载持久草稿，后加入者同步活动房间；只有原本未初始化的文档才使用受信任的结构投影生成一次初始 Yjs 状态。正文不从客户端 HTML/JSON 重置。已保存需要实际入库回执且覆盖目标快照，包括删除集；点击保存固定当时进度，正式历史版本由应用既有版本能力创建。回退同步活动房间的新代际。

`pnpm install --frozen-lockfile --ignore-scripts` 准备本项目自己的依赖；`pnpm build` 编译本项目源码，`pnpm check` 单独检查未构建的源码。构建已经完成类型检查时不重复运行 check。Node 固定为 22.23.2，pnpm 固定为 9.15.6，不从应用工作树复制 node_modules。

Docker 运行固定镜像 `node:22.23.2-bookworm-slim@sha256:48e4b67d85f87bd551df43704e24d252f56cc5f8e9718841aace50f19948f0f9`，只读挂载本项目已编译程序及固定依赖。Compose 配置位于 docker/compose.yaml；实例必须显式提供程序、密钥文件引用、服务配置和已登记的接口网络。没有默认数据库、网络、端口、密钥或运行 fallback。公共 TLS/WSS 入口可以使用已有反向代理，应用源码不进入服务容器。

独立启停入口为 `python3 scripts/service.py start|status|stop --environment private/docker.env`。仓库根取实际 `scripts/service.py` 文件所在仓库，不取调用 cwd；在其他目录可用 `python3 /你的clone目录/scripts/service.py status --environment private/docker.env`。配置文件引用和其中的宿主路径均相对此仓库根解析，拒绝绝对输入、任何 `..` 分量和符号链接路径（包括仍指向仓库内的链接），不得引用仓库外的配置或密钥。容器内固定绝对路径不随 clone 位置改变。

将 `docker/environment.example`、`docker/runtime.example` 分别保存为本服务仓库的 `private/docker.env`、`private/runtime.env`，权限均为 0600；按应用已授权的固定配置填写下表，不在示例中填写真实凭据。入口先校验路径，再只向 Compose 子进程传规范化的宿主路径；不改写配置，也不依赖 shell 的工作目录或同名环境变量覆盖。通过本入口启停，不能将仓库根相对配置直接交给以 docker/ 为基准的 `docker compose up`。

| docker.env 字段 | 含义 |
| --- | --- |
| `COMPOSE_PROJECT_NAME` | 本实例唯一的 Compose 项目名，显式填写。 |
| `COLLABORATION_PROGRAM_ROOT` | 固定为 `.`，仅挂载当前服务仓库。 |
| `COLLABORATION_SECRET_DIRECTORY` | 固定为 `private/collaboration/service`；放 service-secret 与 ca.pem。 |
| `COLLABORATION_RUNTIME_ENV_FILE` | 固定为 `private/runtime.env`，此文件须为普通、单链接、0600 文件。 |
| `COLLABORATION_NETWORK` | 应用已登记的外部接口网络，显式填写。 |

| runtime.env 字段 | 含义 |
| --- | --- |
| `COLLABORATION_PHP_ORIGIN` | 应用私有 HTTPS 接口 origin，禁止关闭 TLS 校验。 |
| `COLLABORATION_INSTANCE_ID` / `COLLABORATION_SERVICE_INSTANCE_ID` | 应用实例与独立服务身份，须与应用配对配置一致。 |
| `COLLABORATION_KEY_ID` | 既有服务密钥的标识；不是密钥值。 |
| `COLLABORATION_EDITION` | 应用固定的部署形态。 |
| `COLLABORATION_CONFIG_REVISION` / `COLLABORATION_CONFIG_SHA256` | 应用确认的配置修订与摘要。 |
| `COLLABORATION_CA_SHA256` | 挂载 ca.pem 的 SHA-256，启动时验证。 |
| `COLLABORATION_PHP_TIMEOUT_MS` | HTTPS 请求超时，毫秒；示例为 20000。 |
| `COLLABORATION_LISTEN_ADDRESS` / `COLLABORATION_LISTEN_PORT` | 已授权的容器内监听地址和端口，无默认值。 |

service-secret 保持普通、单链接、0600、容器 UID 33 所有的文件；ca.pem 保持原 CA 权限、大小和摘要校验。Compose 仍将这两个文件所在目录只读挂载到 `/run/peanut-collaboration`，密钥与 CA 的容器内引用保持不变。不得复制票据密钥或数据库凭据。

应用原生配置入口可按已授权固定配置准备本服务自己的相对路径配置；应用用于定位独立服务仓库的外部 `service_root` 属于应用部署合同，不能据此放宽本服务的仓内路径边界。stop 额外要求原始容器身份与 PHP 已确认的持久排空回执（有界 base64 JSON），再核对同一容器实际停止且退出码为 0。它不强杀、不删卷、不替应用执行授权或修改业务数据；最终 stopped 由应用消费原始签名停止证明后确认。

程序构建、容器健康与应用允许接纳编辑分别记录。服务启动后先完成 PHP/Node 配对，应用明确激活后才接纳编辑；停止须沿用原排空身份、实际持久确认和精确容器停止证明，未知结果先对账。目录调整不重做已成功的旧排空，不重置测试授权或失败预算。源码检查与构建不能代替双人编辑、保存和重新打开的真实验收。
