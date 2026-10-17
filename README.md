# peanut-hocuspocus

与 peanut-admin-code 同级的独立实时协作服务，使用锁定的 Hocuspocus 4.6.0 与 Yjs 13.6.32。完整服务源码位于 src，依赖解析固定在 pnpm-lock.yaml；不随目录调整升级依赖。

服务只通过受认证的 HTTPS 接口取得服务端核实的编辑者身份、连接授权、最新持久化 Yjs 草稿和实际保存回执。应用 PHP 负责统一身份、功能权限、文档数据权限、文件引用、幂等、状态比较与历史版本；服务不连接业务 SQL，不导入或挂载应用 PHP、前端源码。

原生房间先合并和广播，随后批量自动保存。首位加入者加载持久草稿，后加入者同步活动房间；只有原本未初始化的文档才使用受信任的结构投影生成一次初始 Yjs 状态。正文不从客户端 HTML/JSON 重置。已保存需要实际入库回执且覆盖目标快照，包括删除集；点击保存固定当时进度，正式历史版本由应用既有版本能力创建。回退同步活动房间的新代际。

`pnpm install --frozen-lockfile --ignore-scripts` 准备本项目自己的依赖；`pnpm build` 编译本项目源码，`pnpm check` 单独检查未构建的源码。构建已经完成类型检查时不重复运行 check。Node 固定为 22.23.2，pnpm 固定为 9.15.6，不从应用工作树复制 node_modules。

`docker/compose.yaml` 使用固定 Node 镜像，通过本地目录绑定挂载本仓库程序和依赖，不创建 Docker 命名数据卷。运行配置统一放 `docker/.env`，证书和认证密钥放同级的 `certs/`。路径在 Compose 中固定为仓库内相对路径，克隆到其他目录无需修改。

在仓库根目录准备配置并构建：

```sh
cp docker/.env.example docker/.env
chmod 600 docker/.env
mkdir -p certs
# 编辑 docker/.env，填写宿主机端口、应用接口和已有 Docker 网络，导入应用给出的身份与摘要。
pnpm install --frozen-lockfile --ignore-scripts
pnpm build
```

私密文件只放在本服务自己的目录中，不提交到 Git：

```text
certs/
├── service-secret
└── ca.pem
```

`service-secret` 是 API 通信认证密钥，不是 TLS 私钥；保持普通、单链接、0600、容器 UID 33 所有。`ca.pem` 用于验证应用 PHP HTTPS 接口，保持普通、单链接、不可被组或其他用户写入。`certs/` 自身不可被组或其他用户写入，并须允许容器 UID 33 进入，例如目录权限 0755，或由 UID 33 持有且权限 0700。两者只读挂载到容器内 `/run/peanut-collaboration`，程序继续校验文件及 CA 摘要；不挂载应用源码、数据库凭据或应用票据密钥。

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

实例 ID、服务 ID、密钥编号参与 API 签名、房间归属及撤权校验；版本类型、配置修订号和配置摘要用于拒绝过期实例及混用配置的保存、排空和停止请求。它们是当前接口协议的绑定字段，不代表用户需要独立决策七个值。应用托管入口会自动填写身份和摘要；部署者先复制模板并确定宿主机端口，再由应用发布该同一份 `.env`。手动部署时使用应用给出的值。容器内地址、端口和 20 秒请求超时已固定在 Compose 中，不再作为用户配置项。

程序启动后自动通过 API 校验应用身份和配置，应用按原协议激活编辑接纳。容器启动、健康检查和实际编辑保存分别记录，源码构建不能代替双人编辑、保存和重新打开的真实验收。

应用托管的实例继续通过应用生命周期入口执行停止：先关闭编辑接纳并取得持久排空回执，再由应用调用 Compose 停止同一容器，并核验原容器身份、退出状态及签名停止证明。该流程在应用端，不再依赖本仓库的 Python 包装脚本。原有成功排空、未决请求、失败预算和实例身份均保留；不要用强杀或删卷替代未完成的持久保存。非托管维护需要等待 Node 的 SIGTERM 排空时，使用 `docker compose stop --timeout -1 collaboration`，保存失败或状态未知时先对账，不将停止观察当作业务保存成功。
