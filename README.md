# peanut-hocuspocus

与 peanut-admin-code 同级的独立实时协作服务，使用锁定的 Hocuspocus 4.6.0 与 Yjs 13.6.32。完整服务源码位于 src，依赖解析固定在 pnpm-lock.yaml；不随目录调整升级依赖。

服务只通过受认证的 HTTPS 接口取得服务端核实的编辑者身份、连接授权、最新持久化 Yjs 草稿和实际保存回执。应用 PHP 负责统一身份、功能权限、文档数据权限、文件引用、幂等、状态比较与历史版本；服务不连接业务 SQL，不导入或挂载应用 PHP、前端源码。

原生房间先合并和广播，随后批量自动保存。首位加入者加载持久草稿，后加入者同步活动房间；只有原本未初始化的文档才使用受信任的结构投影生成一次初始 Yjs 状态。正文不从客户端 HTML/JSON 重置。已保存需要实际入库回执且覆盖目标快照，包括删除集；点击保存固定当时进度，正式历史版本由应用既有版本能力创建。回退同步活动房间的新代际。

`pnpm install --frozen-lockfile --ignore-scripts` 准备本项目自己的依赖；`pnpm build` 编译本项目源码，`pnpm check` 单独检查未构建的源码。构建已经完成类型检查时不重复运行 check。Node 固定为 22.23.2，pnpm 固定为 9.15.6，不从应用工作树复制 node_modules。

Docker 运行固定镜像 `node:22.23.2-bookworm-slim@sha256:48e4b67d85f87bd551df43704e24d252f56cc5f8e9718841aace50f19948f0f9`，只读挂载本项目已编译程序及固定依赖。Compose 配置位于 docker/compose.yaml；实例必须显式提供程序、密钥文件引用、服务配置和已登记的接口网络。没有默认数据库、网络、端口、密钥或运行 fallback。公共 TLS/WSS 入口可以使用已有反向代理，应用源码不进入服务容器。

独立启停入口为 `python3 scripts/service.py start|status|stop --environment /明确路径/private/docker.env`。应用原生配置入口可按已授权固定配置准备本服务自己的 private/docker.env、private/runtime.env、只读 service-secret 和 CA；不复制票据密钥或数据库凭据。stop 额外要求原始容器身份与 PHP 已确认的持久排空回执（有界 base64 JSON），再核对同一容器实际停止且退出码为 0。它不强杀、不删卷、不替应用执行授权或修改业务数据；最终 stopped 由应用消费原始签名停止证明后确认。

程序构建、容器健康与应用允许接纳编辑分别记录。服务启动后先完成 PHP/Node 配对，应用明确激活后才接纳编辑；停止须沿用原排空身份、实际持久确认和精确容器停止证明，未知结果先对账。目录调整不重做已成功的旧排空，不重置测试授权或失败预算。源码检查与构建不能代替双人编辑、保存和重新打开的真实验收。
