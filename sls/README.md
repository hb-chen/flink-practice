## 阿里云SLS日志分析

- 实时消费SLS日志
- 分析结果写入ES

## ES创建索引
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