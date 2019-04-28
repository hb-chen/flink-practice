package com.hbchen.sls;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.data.RawLogGroupList;
import com.aliyun.openservices.log.flink.data.RawLogGroupListDeserializer;
import com.aliyun.openservices.log.flink.util.Consts;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.yaml.snakeyaml.constructor.Constructor;

import java.util.*;
import javax.annotation.Nullable;

import com.hbchen.sls.config.Config;
import com.hbchen.sls.util.AccessLogAnalysis;

public class SlsAnalysis {
    private static final Logger LOG = org.apache.log4j.Logger.getLogger(SlsAnalysis.class);

    public static void main(String[] args) throws Exception {
        Config config = getConfig();

        // SLS消费
        // https://help.aliyun.com/document_detail/63594.html
        Properties configProps = new Properties();
        // 设置访问日志服务的域名
        configProps.put(ConfigConstants.LOG_ENDPOINT, "cn-hangzhou.log.aliyuncs.com");
        // 设置访问ak
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID, config.getSls().getAk());
        configProps.put(ConfigConstants.LOG_ACCESSKEY, config.getSls().getSk());
        // 设置日志服务的project
        configProps.put(ConfigConstants.LOG_PROJECT, config.getSls().getProject());
        // 设置日志服务的Logstore
        configProps.put(ConfigConstants.LOG_LOGSTORE, config.getSls().getLogStore());
        // 设置消费日志服务起始位置
        // Consts.LOG_BEGIN_CURSOR： 表示从shard的头开始消费，也就是从shard中最旧的数据开始消费。
        // Consts.LOG_END_CURSOR： 表示从shard的尾开始，也就是从shard中最新的数据开始消费。
        // Consts.LOG_FROM_CHECKPOINT：表示从某个特定的ConsumerGroup中保存的Checkpoint开始消费，通过ConfigConstants.LOG_CONSUMERGROUP指定具体的ConsumerGroup。
        // UnixTimestamp： 一个整型数值的字符串，用1970-01-01到现在的秒数表示， 含义是消费shard中这个时间点之后的数据。
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, "" + (System.currentTimeMillis() / 1000L));
        // 设置日志拉取时间间隔及每次调用拉取的日志数量
        configProps.put(ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS, "1000");
        configProps.put(ConfigConstants.LOG_MAX_NUMBER_PER_FETCH, "100");
        // 设置Shards发现周期
        configProps.put(ConfigConstants.LOG_SHARDS_DISCOVERY_INTERVAL_MILLIS, Consts.DEFAULT_SHARDS_DISCOVERY_INTERVAL_MILLIS);

        // 设置日志服务的消息反序列化方法
        RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RawLogGroupList> logStream = env.addSource(
                new FlinkLogConsumer<RawLogGroupList>(deserializer, configProps));

        // Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 开启flink exactly once语义
        // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 每5s保存一次checkpoint
        // env.enableCheckpointing(5000);

        // SLS批量日志展开
        DataStream<Tuple4<AccessLogAnalysis, Integer, Long, Long>> flatStream = logStream.flatMap(new FlatMapFunction<RawLogGroupList, Tuple4<AccessLogAnalysis, Integer, Long, Long>>() {
            @Override
            public void flatMap(RawLogGroupList value, Collector<Tuple4<AccessLogAnalysis, Integer, Long, Long>> out) throws Exception {
                // Log group内时间升序，记录所有分组最小时间戳，即为watermark位置
                long minTimestamp = Long.MAX_VALUE;
                for (RawLogGroup group : value.getRawLogGroups()) {
                    if (group.getLogs().size() > 0) {
                        RawLog log = group.getLogs().get(0);
                        long rt = Optional.ofNullable(log.getContents().get("request_time")).map(Long::new).orElse(Long.MAX_VALUE);
                        minTimestamp = rt < minTimestamp ? rt : minTimestamp;
                    }
                }
                // 取毫秒，排除MAX_VALUE
                minTimestamp = minTimestamp == Long.MAX_VALUE ? minTimestamp : minTimestamp * 1000;

                Integer count = 0;
                for (RawLogGroup group : value.getRawLogGroups()) {
                    count += group.getLogs().size();
                    for (RawLog log : group.getLogs()) {
                        String path = log.getContents().get("path");
                        if (path.equalsIgnoreCase("/analysis/path")) {
                            AccessLogAnalysis ala = new AccessLogAnalysis();

                            // 解析query参数
                            QueryStringDecoder decoder = new QueryStringDecoder(log.getContents().get("uri"));
                            List<String> idList = decoder.parameters().get("id");
                            if (idList != null && idList.size() > 0) {
                                Integer id = Optional.ofNullable(idList.get(0)).map(Integer::new).orElse(0);
                                ala.setKey(id);
                            } else {
                                continue;
                            }

                            ala.setPath(path);
                            Long timestamp = Optional.ofNullable(log.getContents().get("request_time")).map(Long::new).orElse(0L);
                            timestamp *= 1000;

                            out.collect(new Tuple4<>(ala, 1, timestamp, minTimestamp));
                        }

                    }
                }
                LOG.info("raw log count:" + count
                        + " timestamp:" + minTimestamp);
            }
        });

        DataStream result = flatStream.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple4<AccessLogAnalysis, Integer, Long, Long>>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(Tuple4<AccessLogAnalysis, Integer, Long, Long> lastElement, long extractedTimestamp) {
                return lastElement.f3 <= extractedTimestamp ? new Watermark(lastElement.f3) : null;
            }

            @Override
            public long extractTimestamp(Tuple4<AccessLogAnalysis, Integer, Long, Long> element, long previousElementTimestamp) {
                if (element.f2 > 0L) {
                    return element.f2;
                } else {
                    return previousElementTimestamp;
                }
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(60))
                .sum(1);

        if (false) {
            result.print();
            result.addSink(new SinkFunction<Tuple4<AccessLogAnalysis, Integer, Long, Long>>() {
                @Override
                public void invoke(Tuple4<AccessLogAnalysis, Integer, Long, Long> value, Context context) throws Exception {
                    LOG.info("path:" + value.f0.getPath()
                            + " count:" + value.f1
                            + " timestamp" + value.f2
                            + " ctx.timestamp:" + context.timestamp()
                            + " ctx.watermark:" + context.currentWatermark());
                }
            });
        } else {
            // Sink:Elasticsearch
            List<HttpHost> httpHosts = new ArrayList<>();
            httpHosts.add(new HttpHost(config.getEs().getHostname(), 9200, "http"));

            // use a ElasticsearchSink.Builder to create an ElasticsearchSink
            ElasticsearchSink.Builder<Tuple4<AccessLogAnalysis, Integer, Long, Long>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                    httpHosts,
                    new ElasticsearchSinkFunction<Tuple4<AccessLogAnalysis, Integer, Long, Long>>() {
                        public IndexRequest createIndexRequest(Tuple4<AccessLogAnalysis, Integer, Long, Long> element) {
                            Map<String, String> json = new HashMap<>();
                            json.put("path", element.f0.getPath());
                            json.put("key", element.f0.getKey().toString());
                            json.put("count", element.f1.toString());
                            json.put("timestamp", element.f3.toString());

                            return Requests.indexRequest()
                                    .index("sls_analysis")
                                    .type("_doc")
                                    .source(json);
                        }

                        @Override
                        public void process(Tuple4<AccessLogAnalysis, Integer, Long, Long> element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                            requestIndexer.add(createIndexRequest(element));
                        }
                    }
            );

            // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
            esSinkBuilder.setBulkFlushMaxActions(1);

            // provide a RestClientFactory for custom configuration on the internally created REST client
            String esUsername = config.getEs().getUsername();
            String esPassword = config.getEs().getPassword();
            esSinkBuilder.setRestClientFactory(
                    restClientBuilder -> {
                        // restClientBuilder.setDefaultHeaders(headers);
                        // restClientBuilder.setMaxRetryTimeoutMillis(...)
                        // restClientBuilder.setPathPrefix(...)
                        restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esUsername, esPassword));
                                return httpAsyncClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider);
                            }
                        });
                    }
            );

            // finally, build and add the sink to the job's pipeline
            result.addSink(esSinkBuilder.build());
        }

        env.execute();
    }

    private static Config getConfig() {
        Constructor constructor = new Constructor(Config.class);
        org.yaml.snakeyaml.Yaml yaml = new org.yaml.snakeyaml.Yaml(constructor);

        Config config = yaml.loadAs(SlsAnalysis.class.getClassLoader().getResourceAsStream("config.yml"), Config.class);
        return config;
    }
}