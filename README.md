# peanut-hocuspocus

独立的 Hocuspocus 实时协作服务，锁定 Hocuspocus4.6.0、Yjs13.6.32、官方Node22.23.2和pnpm9.15.6。通过配置的HTTP/HTTPS地址请求应用后端，不要求同机、同Compose或同Docker网络；不访问业务数据库、不挂应用源码，不另存一份文档数据。

前端携带token和文档标识建立WebSocket，每个连接经后端验证身份及文档权限。首次加载持久化文档，后续成员同步活动房间；普通文本增量在HP合并广播，新增文件引用仍须授权。自动保存和手动保存沿用同一提交回执合同；保留授权续验、撤权、CAS、原请求重试、未保存保护和安全停止。广播成功不等于已保存。

## 目录与配置

```text
peanut-hocuspocus/
├── src/                      # 仅开发源码
├── dist/                     # 编译产物，容器实际执行
├── node_modules/             # 本项目依赖
├── config/api.json           # 默认v1/action映射，通常不改
├── compose.yaml              # 根目录标准启动入口
├── .env                      # 实际配置，0600，不提交
├── .env.example              # 公开模板，无真实密钥
├── docker/                   # 可选私有CA、外部网络示例
├── package.json
├── pnpm-lock.yaml
└── tsconfig.json             # 仅开发编译使用
```

从应用取得配套配置，保存为根目录`.env`并设为0600。部署者主要确认：

```dotenv
BASE_URL=https://app.example.com/adminapi/internal/collaboration
SERVICE_SECRET='应用提供的至少32字节服务认证密钥'
```

现有签名协议仍要求应用提供`INSTANCE_ID`、`SERVICE_INSTANCE_ID`、`KEY_ID`，写在同一份`.env`中。完整应用配套配置应已填好这些身份；不能省略、猜测或重启时重新生成。简化手工填写不等于删除身份校验。旧Code配置发布器仍生成旧变量及目录，采用本版本前须由应用owner更新直接消费者，或依据相同配对值准备新`.env`；旧文件不能直接当作新配置。

Compose读取`.env`注入环境，运行容器不挂整个仓库或`.env`文件。固定镜像、UID/GID33、内部端口80、只读权限和健康检查写在Compose中。默认网络由Compose自动创建，无需填写网络名。读取配置即监听，不执行全局注册、prepare、activate或主动配对探测。

## API默认路径与斜杠

`BASE_URL`包含`v1`之前的路径；`config/api.json`提供默认映射：

| 操作 | 相对路径 |
| --- | --- |
| 认证 | `v1/authorize` |
| 续验 | `v1/check` |
| 加载 | `v1/load` |
| 保存 | `v1/store` |
| 原回执 | `v1/receipt` |
| 文件引用 | `v1/references` |
| 房间恢复 | `v1/recovery` |
| 保存目标准备 | `v1/target/prepare` |
| 保存目标操作 | `v1/target` |

例如`https://app.example.com/adminapi/internal/collaboration`加`v1/authorize`得到`/adminapi/internal/collaboration/v1/authorize`。基地址末尾规范为一个`/`，接口路径开头的`/`被移除。使用URL解析器拼接，不替换`https://`中的斜杠；只允许已知操作和受限相对路径，禁止换域名、查询参数或通过`..`越出基路径。请求和响应签名使用实际请求的完整pathname；覆盖地址不改变请求体、签名、权限或保存协议，后端必须支持对应路径。

## 本地开发与启动

按本仓锁文件准备依赖，`pnpm build`编译到`dist/`。构建已包含类型检查时不重复运行check；只做类型检查、不输出产物时用`pnpm check`。不复制其他仓依赖。

已有构建、依赖和完整`.env`后，在项目根执行：

```sh
docker compose config --quiet
docker compose up -d
docker compose ps
docker compose logs -f collaboration
```

默认发布`127.0.0.1:1234`，容器内`0.0.0.0:80`。只读挂载`dist/`、`node_modules/`、`package.json`和`config/`到`/app`，执行`dist/src/main.js`。源码变化先构建再重启；环境变量或挂载变化需重新创建容器。运行时不安装依赖、不下载源码、不编译。旧容器不会因仓库文件变化自动采用新配置。

## 无源码发行与服务器

源码固定、干净并完成对应构建和资格检查后：

```sh
npm run package -- /absolute/new-release-directory
```

打包器只消费已有构建，不构建、测试、安装、发布或操作容器。发行目录及相邻`.tar.gz`包含`dist/`、`config/api.json`、Compose及高级示例、包清单、锁文件、README、LICENSE和文件摘要清单，另提供归档SHA-256文件。不包含`src/`、Git、真实配置、证书私钥、node_modules、TypeScript源码或source map。包清单保留与锁一致的依赖声明，但移除开发命令；devDependencies声明不等于服务器安装编译器。

服务器解压后，用固定工具链执行`pnpm install --prod --frozen-lockfile --ignore-scripts`准备目标平台的生产依赖，再放入应用提供的完整`.env`，通过相同的Compose启动。依赖准备也可在官方Node容器中完成，服务启动不暗中执行这一步。服务器不需要源码、tsconfig或TypeScript编译。文件摘要不冒充构建来源证明或业务资格，正式采用仍须对应的固定构建回执。

## 高级选项

| 参数 | 默认/用途 |
| --- | --- |
| `PORT` | 宿主机端口1234，与容器内部端口独立 |
| `BIND_ADDRESS` | 宿主机绑定127.0.0.1，部署者明确选择访问范围 |
| `REQUEST_TIMEOUT_MS` | 后端超时20000ms，范围100–30000 |
| `INSTANCE_ID`、`SERVICE_INSTANCE_ID`、`KEY_ID` | 应用配套提供的协议身份 |

普通公网HTTPS使用Node正常信任链，无需CA文件。HTTP/HTTPS按显式地址选择，不自动从HTTPS降级；HTTP没有传输加密。用户token不放URL或日志；服务密钥与用户token不同。

私有HTTPS：在`certs/ca.pem`放应用提供的公共CA，文件须普通、单链接、不可被组或其他用户写入；在`.env`增加应用提供的`CA_SHA256`，然后：

```sh
docker compose -f compose.yaml -f docker/ca.example.yaml up -d
```

示例只读挂载CA并设置`NODE_EXTRA_CA_CERTS`；程序核验文件、权限、大小和固定摘要。不关闭证书验证，不修改系统或浏览器信任。WSS服务器证书和私钥由独立TLS入口管理。

仅跨Compose容器别名互访时，在`.env`提供`NETWORK`并显式选用`docker/network.example.yaml`。默认不选此文件、不填NETWORK。项目名通常取目录名，多实例使用标准`docker compose -p <name>`。

## 保存、健康和停止

`/live`表示正在监听且未停止，是容器健康检查入口。`/ready`反映最近受认证后端请求的状态，未知、失联或观察过期返回503，不主动探测，也不因启动而伪报已配对。后端不可达时Node保持监听；新连接不能跳过认证，已有授权受有效期与续验约束。

SIGTERM/SIGINT停止新接纳，等待在途消息、保存及真实回执后退出。维护停止使用`docker compose stop --timeout -1 collaboration`；保存失败或未知不能强杀、清房间、换请求或假称已排空。正常提交并停止后，重开从后端恢复；突然杀进程不能靠挂载找回内存中的未确认编辑。

切换旧实例前核对身份、挂载和恢复材料，正常排空后由有权限的owner重建。源码交付、构建、容器采用和业务验收分别记录；原始失败与数据不因新配置清除。
