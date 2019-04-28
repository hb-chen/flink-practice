# SLS日志分析

- 实时消费`SLS`日志
- 分析结果输出到`ES`
- 技术点
    - 批量数据展开：`flatMap()`
    - 自定义`EventTime`、`Watermark`：`assignTimestampsAndWatermarks()`
    - 滚动窗口：`timeWindow()`

## 假设场景
- `SLS`日志结构
    - `uri`
    - `request_time`，秒级
- 仅统计指定`path`
    - `/analysis/path`
    - 且`uri`的`query`参数中有内容`id`需要解析，`/analysis/path?id=1`
- ES索引

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
    
```bash
GET /sls_analysis/_search
```