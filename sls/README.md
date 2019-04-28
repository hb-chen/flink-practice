## 阿里云SLS日志分析

- 实时消费SLS日志
- 分析结果写入ES

## 假设场景
- `SLS`日志结构
    - `uri`
    - `request_time`，秒级
- 仅统计指定`path`
    - `/analysis/path`
    - 且`uri`的`query`参数中有内容`id`需要解析，`/analysis/path?id=1`
- ES索引

### ES创建索引
```json
PUT sls_analysis

PUT sls_analysis/_mapping/_doc
{
  "properties": {
    "path": {
      "type": "text"
    },
    "key": {
      "type": "integer"
    },
    "count": {
      "type": "integer"
    },
    "timestamp": {
      "type":"long"
    }
  }
}
```