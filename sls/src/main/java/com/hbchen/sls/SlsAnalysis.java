package com.hbchen.sls;

import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.data.RawLogGroupList;
import com.aliyun.openservices.log.flink.data.RawLogGroupListDeserializer;
import com.aliyun.openservices.log.flink.util.Consts;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class SlsAnalysis {
    public static void main(String[] args) throws Exception {
        Properties configProps = new Properties();
        // 设置访问日志服务的域名
        configProps.put(ConfigConstants.LOG_ENDPOINT, "cn-hangzhou.log.aliyuncs.com");
        // 设置访问ak
        configProps.put(ConfigConstants.LOG_ACCESSSKEYID, "");
        configProps.put(ConfigConstants.LOG_ACCESSKEY, "");
        // 设置日志服务的project
        configProps.put(ConfigConstants.LOG_PROJECT, "ali-cn-hangzhou-sls-admin");
        // 设置日志服务的Logstore
        configProps.put(ConfigConstants.LOG_LOGSTORE, "sls_consumergroup_log");
        // 设置消费日志服务起始位置
        // configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, Consts.LOG_END_CURSOR);
        configProps.put(ConfigConstants.LOG_CONSUMER_BEGIN_POSITION, "" + (System.currentTimeMillis() / 1000L - 3 * 60));
        // 设置日志服务的消息反序列化方法
        RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RawLogGroupList> logStream = env.addSource(
                new FlinkLogConsumer<RawLogGroupList>(deserializer, configProps));

        // 开启flink exactly once语义
        // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 每5s保存一次checkpoint
        // env.enableCheckpointing(5000);

        DataStream<Tuple2<String, Integer>> counts = logStream
                .flatMap(new FlatMapFunction<RawLogGroupList, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(RawLogGroupList value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (RawLogGroup group : value.getRawLogGroups()) {
                            for (RawLog log : group.getLogs()) {
                                String key = log.getContents().get("path");
                                out.collect(new Tuple2<>(key, 1));
                            }
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(30))
                .sum(1);

        counts.print();

        env.execute();
    }
}
