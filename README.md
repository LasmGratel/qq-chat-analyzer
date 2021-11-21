# QQ 聊天记录分析

分析纯文本格式的 QQ 聊天记录，导出为 SQLite（等）格式，并分析分类消息记录中的所有用户

## 使用

打开QQ消息管理器，右上角倒三角导出全部消息记录到一个txt文件

如果使用 SQLite 请准备好全部消息记录*2的硬盘空间，10G的消息记录需要20G的可用空间来存放数据库（当然之后可以zip压缩减少很大部分）

设置环境变量 DATABASE_URL 为你的数据库连接，例如 `mysql://localhost/my_database` 或者 `sqlite://msg.db?mode=rwc`

在 PowerShell 下可以使用 `$env:DATABASE_URL=` 来设置该环境变量

运行 `qq-chat-analyzer.exe <你的txt文件位置>`，出现进度条后则说明运行成功，开始分析
