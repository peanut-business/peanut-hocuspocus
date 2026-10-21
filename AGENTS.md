# peanut-hocuspocus

独立 Hocuspocus 实时协作服务。日常集成分支为 dev，源码在功能 worktree 修改；保留他人修改，不改 main/tag 或升级依赖。

使用本项目 package.json、pnpm-lock.yaml 和固定官方 Node 22.23.2 镜像。认证、文档加载、实际保存、文件引用与版本业务通过显式 BASE_URL 对应的受认证 HTTP/HTTPS 接口完成；不要求后端同机或共用 Docker 网络，不自动降级 HTTPS。禁止导入、挂载或执行应用 PHP/前端源码，禁止直接连接业务数据库。

根目录 compose.yaml 是本地与无源码发行物的统一标准入口，运行配置和服务密钥放忽略的根 .env。BASE_URL 包含版本之前的路径，config/api.json 保存默认 v1/action相对路径；规范化拼接斜杠，不允许切换域名或越出基路径。普通配置不要求填写网络名或项目名；高级外部网络和私有 CA 使用 docker/ 下的显式示例。私有 CA 保留正常证书验证及文件/摘要校验，不导入系统信任。容器内固定80，宿主机默认127.0.0.1:1234。精确只读挂载 dist、node_modules、package.json、config，不挂整仓，不增加启动包装脚本或文档数据卷。生产依赖按目标平台锁文件准备；发行物不含源码、秘密、Git、开发依赖目录及 source map。打包不隐式构建、安装、测试或部署。

Peanut 维护者按 Git common-dir 与 Project repos.yaml 核对归属；维护者资源事实源为 peanut-admin-project/resources/project-resources.json，操作前读取精确资源和租约。公开用户不依赖该私有 Project。读取配置即监听；每次连接仍须后端验证用户 token 和租户/文档权限，不依赖全局准备或激活。实际入库回执才代表保存成功。沿用原测试授权与失败预算，不自行启动测试套件。
