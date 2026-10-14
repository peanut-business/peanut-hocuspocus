# peanut-hocuspocus

独立 Hocuspocus 实时协作服务。日常集成分支为 dev，源码在功能 worktree 修改；保留他人修改，不改 main/tag 或升级依赖。

使用本项目 package.json、pnpm-lock.yaml 和固定 Node 22.23.2 镜像。认证、文档加载、实际保存、文件引用与版本业务通过已安装应用的受认证 HTTPS 接口完成。禁止导入、挂载或执行应用 PHP/前端源码，禁止直接连接业务数据库。

运行配置与密钥仅放忽略的 private 目录或明确的只读外部引用，不提交密钥、日志、数据和 node_modules。容器只读挂载本项目的程序及固定依赖，不把应用源代码烘焙进镜像。

Peanut 维护者按 Git common-dir 与 Project repos.yaml 核对归属；维护者资源事实源为 peanut-admin-project/resources/project-resources.json，操作前读取精确资源和租约。公开用户不依赖该私有 Project。启动不代表应用已授权接纳编辑，实际入库回执才代表保存成功。沿用原测试授权与失败预算，不自行启动测试套件。
