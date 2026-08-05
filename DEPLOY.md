# A股量化监控 V4 - 部署指南

## V4 核心升级

- **多源聚合**: AKShare + 东方财富 + 新浪财经，自动故障转移
- **实时推送**: WebSocket 30秒级增量更新
- **单文件前端**: CSS/JS 全部内联，无需静态文件服务器
- **零成本运行**: 纯 Python，无需 Redis/数据库

## 快速启动

### 方式一：Docker（推荐）

```bash
cd a-stock-quant-monitor-v4
docker-compose up -d
```

访问: http://localhost:10000

### 方式二：本地运行

```bash
cd a-stock-quant-monitor-v4/backend
pip install -r requirements.txt
python app.py
```

### 方式三：国内云服务器部署

```bash
# 1. 购买阿里云/腾讯云/华为云 ECS（中国大陆节点）
# 2. SSH 登录
ssh root@你的服务器IP

# 3. 安装 Docker
curl -fsSL https://get.docker.com | sh
systemctl enable docker && systemctl start docker

# 4. 克隆并启动
git clone https://github.com/你的用户名/a-stock-quant-monitor-v4.git
cd a-stock-quant-monitor-v4
docker-compose up -d

# 5. 访问
http://你的服务器IP:10000
```

## 数据源说明

| 数据源 | 用途 | 优先级 |
|--------|------|--------|
| AKShare | 全市场/板块/北向/龙虎榜 | 1 |
| 东方财富 HTTP API | 指数实时/全市场/K线 | 2 |
| 新浪财经 | 指数实时 | 3 |
| Mock 数据 | 兜底 | 4 |

**国内服务器**: 优先使用 AKShare，数据最全面  
**海外服务器**: 自动降级到东方财富/新浪 API，部分功能使用 Mock

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| HOST | 0.0.0.0 | 监听地址 |
| PORT | 10000 | 端口 |
| UPDATE_INTERVAL | 30 | 刷新间隔(秒) |
| DEBUG | false | 调试模式 |

## API 端点

- `GET /` - 前端页面
- `GET /api/data` - 全量数据
- `GET /api/market?limit=50&offset=0` - 市场扫描分页
- `GET /api/stock/{symbol}/detail` - 个股详情+K线
- `GET /api/stock/{symbol}/kline?days=60` - K线数据
- `WS /ws` - WebSocket 实时推送
