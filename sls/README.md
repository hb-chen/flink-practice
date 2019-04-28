# SLS日志分析

- 实时消费`SLS`日志
- 分析结果输出到`ES`
- 技术点
    - 批量数据展开：`flatMap()`
    - 自定义`EventTime`、`Watermark`：`assignTimestampsAndWatermarks()`
    - 滚动窗口：`timeWindow()`

## 假设场景
### 日志结构
```bash
request_time: 1556415329
uri: /analysis/path?id=1
```
- 字段
    - `uri`
    - `request_time`，秒级
- 筛选指定`path`
    - `/analysis/path`
    - 且`uri`的`query`参数中有内容`id`需要解析，如：`/analysis/path?id=1`

### ES索引
```bash
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

## Run & Build
```bash
$ ./gradlew sls:run --args="--path /analysis/path"
$ ./gradlew clean sls:shadowJar
```

## Submit new Job on Dashboard
- Entry Class
    - com.hbchen.sls.SlsAnalysis
- Program Arguments
    - --path /analysis/path

### 查看ES写入情况    
```bash
GET /sls_analysis/_search
```
```json
{
  "took": 1,
  "timed_out": false,
  "_shards": {
    "total": 5,
    "successful": 5,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": 258,
    "max_score": 1,
    "hits": [
      {
        "_index": "sls_analysis",
        "_type": "_doc",
        "_id": "FKLbY2oBl_E4v-6WGwpr",
        "_score": 1,
        "_source": {
          "path": "/analysis/path",
          "count": "456",
          "key": "123",
          "timestamp": "1556453445000"
        }
      },
    ]
  }
} 
```