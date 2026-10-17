# peanut-hocuspocus

与 peanut-admin-code 同级的独立实时协作服务，使用锁定的 Hocuspocus 4.6.0 与 Yjs 13.6.32。完整服务源码位于 src，依赖解析固定在 pnpm-lock.yaml；不随目录调整升级依赖。

服务只通过受认证的 HTTPS 接口取得服务端核实的编辑者身份、连接授权、最新持久化 Yjs 草稿和实际保存回执。应用 PHP 负责统一身份、功能权限、文档数据权限、文件引用、幂等、状态比较与历史版本；服务不连接业务 SQL，不导入或挂载应用 PHP、前端源码。

原生房间先合并和广播，随后批量自动保存。首位加入者加载持久草稿，后加入者同步活动房间；只有原本未初始化的文档才使用受信任的结构投影生成一次初始 Yjs 状态。正文不从客户端 HTML/JSON 重置。已保存需要实际入库回执且覆盖目标快照，包括删除集；点击保存固定当时进度，正式历史版本由应用既有版本能力创建。回退同步活动房间的新代际。

`pnpm install --frozen-lockfile --ignore-scripts` 准备本项目自己的依赖；`pnpm build` 编译本项目源码，`pnpm check` 单独检查未构建的源码。构建已经完成类型检查时不重复运行 check。Node 固定为 22.23.2，pnpm 固定为 9.15.6，不从应用工作树复制 node_modules。

`docker/compose.yaml` 使用固定 Node 镜像，通过本地目录绑定挂载本仓库程序和依赖，不创建 Docker 命名数据卷。运行配置统一放 `docker/.env`，CA 证书放同级的 `certs/`，服务认证密钥字符串放忽略的 `docker/.env`。路径在 Compose 中固定为仓库内相对路径，克隆到其他目录无需修改。

在仓库根目录准备配置并构建：

```sh
cp docker/.env.example docker/.env
chmod 600 docker/.env
mkdir -p certs
# 编辑 docker/.env，填写宿主机端口、应用接口和已有 Docker 网络，导入应用给出的实例身份、服务密钥和 CA 摘要。
pnpm install --frozen-lockfile --ignore-scripts
pnpm build
```

私密文件只放在本服务自己的目录中，不提交到 Git：

```text
certs/
└── ca.pem
```

`COLLABORATION_SECRET` 是至少 32 字节的服务间 HMAC 密钥字符串，后端使用相同值；它与用户登录 token 独立，不发送到前端或日志，不要求密钥文件。`.env` 保持 0600。`ca.pem` 用于验证后端 HTTPS 接口，保持普通、单链接、不可被组或其他用户写入。`certs/` 自身不可被组或其他用户写入，并须允许容器 UID 33 进入。目录只读挂载到 `/run/peanut-collaboration`，程序校验 CA 摘要，不挂载应用源码或数据库凭据。

服务自身没有文档数据盘：Yjs 房间在内存同步，持久草稿和保存回执通过 PHP API 写入应用数据库，持久化目录沿用应用自己的本地目录挂载。这里不增加 `data/` 副本或第二个持久化入口；容器 `/tmp` 的 tmpfs 仅用于临时文件。

配置和文件准备完成后，使用标准 Compose 命令：

```sh
cd docker
docker compose config --quiet
docker compose up -d
docker compose ps
docker compose logs -f collaboration
```

锁定的 Hocuspocus 4.6.0 默认监听端口为 80，Compose 固定为容器内 `0.0.0.0:80`；`.env` 的 `COLLABORATION_PORT` 只控制宿主机端口，默认 `80:80`，例如填写 `1234` 则映射 `1234:80`。应用反向代理通过 Docker 网络访问 `collaboration:80`，不使用宿主机映射端口。对外的 WS/HTTP 端口不提供 TLS，浏览器 WSS 入口仍由应用已有的 TLS 反向代理提供。容器仅在自己的网络命名空间允许 UID 33 绑定端口 80，仍丢弃全部 Linux capabilities。

`COLLABORATION_PHP_ORIGIN` 是唯一需要配置的应用 API 根地址，例如 `https://app.example`；各操作路径由接口协议固定为 `/adminapi/internal/collaboration/v1/authorize`、`load`、`store` 等，不要求逐接口填写 URL，也不拆成可任意拼接的多个配置项。

实例 ID、服务 ID、密钥编号参与服务 API 签名、房间归属及撤权校验。应用提供这些值及版本类型；进程 holder 由程序生成，房间代际与授权租约由后端维护。部署者无需填写全局 activation、probe、配置修订号或摘要来开放编辑。

程序读取配置后直接监听，不执行注册、prepare、activate 或配对启动。前端携带用户登录 token 和租户/文档上下文；Node 将其通过同一受认证后端 API 转交验证，后端核实用户会话、成员、文档归属及读写权限，返回可信隔离房间和有效期。Node 不信任客户端自报身份。权限续期、动态撤权、文档加载及保存仍使用后端 API；保存成功须取得实际入库回执。

后端不可达时服务保持监听，新连接因无法鉴权而拒绝；已有连接受授权有效期和续期约束，未保存内容仍受原 capture、回执和拒绝卸载保护，不重置失败次数。`/live` 仅表示监听且未停止，用于 Compose 存活检查；`/ready` 显示最近受认证 API 调用的依赖状态，未知或失联返回 503，不主动探测后端。健康检查不能代替编辑保存验收。

SIGTERM/SIGINT 关闭新接纳，等待在途消息、操作和真实保存回执后才销毁服务。保存失败或结果未知时拒绝完成排空，保留内存内容，不强杀或创建新请求来绕过。维护停止使用 `docker compose stop --timeout -1 collaboration`，须在授权窗口内确认安全排空和实际进程终态；旧实例的未决请求、owner、租约和历史预算仍需正常交接，不因新源码自动清除。实际双人同步、保存和重开结果分别验收。
