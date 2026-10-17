# peanut-hocuspocus

与 peanut-admin-code 同级的独立实时协作服务，使用锁定的 Hocuspocus 4.6.0 与 Yjs 13.6.32。完整服务源码位于 src，依赖解析固定在 pnpm-lock.yaml；不随目录调整升级依赖。

服务只通过受认证的 HTTPS 接口取得服务端核实的编辑者身份、连接授权、最新持久化 Yjs 草稿和实际保存回执。应用 PHP 负责统一身份、功能权限、文档数据权限、文件引用、幂等、状态比较与历史版本；服务不连接业务 SQL，不导入或挂载应用 PHP、前端源码。

原生房间先合并和广播，随后批量自动保存。首位加入者加载持久草稿，后加入者同步活动房间；只有原本未初始化的文档才使用受信任的结构投影生成一次初始 Yjs 状态。正文不从客户端 HTML/JSON 重置。已保存需要实际入库回执且覆盖目标快照，包括删除集；点击保存固定当时进度，正式历史版本由应用既有版本能力创建。回退同步活动房间的新代际。

`pnpm install --frozen-lockfile --ignore-scripts` 准备本项目自己的依赖；`pnpm build` 编译本项目源码，`pnpm check` 单独检查未构建的源码。构建已经完成类型检查时不重复运行 check。Node 固定为 22.23.2，pnpm 固定为 9.15.6，不从应用工作树复制 node_modules。

仓库根目录的 `compose.yaml` 使用固定 Node 镜像，只读挂载本仓库的程序和依赖。运行配置统一放根目录 `.env`；各参数已在 `.env.example` 中用中文逐项说明，配置里不需要填写任何宿主机文件路径。Compose 按自身文件位置解析固定的相对挂载路径，克隆到其他目录无需修改路径。

在仓库根目录准备配置并构建：

```sh
cp .env.example .env
chmod 600 .env
# 编辑 .env，填写应用接口、身份、配置摘要、端口和已有 Docker 网络。
pnpm install --frozen-lockfile --ignore-scripts
pnpm build
```

私密文件只放在本服务自己的目录中，不提交到 Git：

```text
private/collaboration/service/
├── service-secret
└── ca.pem
```

`service-secret` 使用与应用约定的服务通信密钥，保持普通、单链接、0600、容器 UID 33 所有；`ca.pem` 是验证应用 PHP HTTPS 接口的 CA 文件，保持普通、单链接、不可被组或其他用户写入。程序继续校验密钥、CA 文件及 CA 摘要。两者只读挂载到容器内 `/run/peanut-collaboration`，不挂载应用源码、数据库凭据或应用票据密钥。

配置和文件准备完成后，使用标准 Compose 命令：

```sh
docker compose config --quiet
docker compose up -d
docker compose ps
docker compose logs -f collaboration
```

服务通过 `.env` 指定的 Docker 网络访问应用 PHP API。当前 Compose 不发布宿主机端口，浏览器的 WSS 入口由应用已有的 TLS 反向代理提供；需要从宿主机直连时须明确配置端口映射，不能把容器监听地址当作宿主机访问地址。

程序启动后自动通过 API 校验应用身份和配置，应用按原协议激活编辑接纳。容器启动、健康检查和实际编辑保存分别记录，源码构建不能代替双人编辑、保存和重新打开的真实验收。

应用托管的实例继续通过应用生命周期入口执行停止：先关闭编辑接纳并取得持久排空回执，再由应用调用 Compose 停止同一容器，并核验原容器身份、退出状态及签名停止证明。该流程在应用端，不再依赖本仓库的 Python 包装脚本。原有成功排空、未决请求、失败预算和实例身份均保留；不要用强杀或删卷替代未完成的持久保存。非托管维护需要等待 Node 的 SIGTERM 排空时，使用 `docker compose stop --timeout -1 collaboration`，保存失败或状态未知时先对账，不将停止观察当作业务保存成功。
