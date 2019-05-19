/*
 * Copyright (c) 2019. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package com.hbchen.sls;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.request.ListShardRequest;
import com.aliyun.openservices.log.response.ListShardResponse;
import org.apache.log4j.Logger;

import java.sql.Timestamp;

import com.hbchen.sls.config.Config;

/**
 * SLS ConsumerGroup操作
 *
 * @author hbchen.com
 */
public class ConsumerGroup {

	private static final Logger LOG = org.apache.log4j.Logger.getLogger(ConsumerGroup.class);

	public static void main(String[] args) throws Exception {
		updateCheckpoint();
	}

	/**
	 * 重置ConsumerGroup的CheckPoint时间点
	 * https://help.aliyun.com/document_detail/28998.html
	 *
	 * @throws Exception
	 */
	public static void updateCheckpoint() throws Exception {
		Config config = SlsAnalysis.getConfig();
		String host = config.getSls().getEndpoint();
		String accessId = config.getSls().getAk();
		String accessKey = config.getSls().getSk();
		String project = config.getSls().getProject();
		String logStore = config.getSls().getLogStore();
		String consumerGroup = "sls-analysis";

		Client client = new Client(host, accessId, accessKey);
		long timestamp = Timestamp.valueOf("2019-05-18 00:00:00").getTime() / 1000;
		ListShardResponse response = client.ListShard(new ListShardRequest(project, logStore));
		for (Shard shard : response.GetShards()) {
			int shardId = shard.GetShardId();
			LOG.info("shared id: " + shardId);
			String cursor = client.GetCursor(project, logStore, shardId, timestamp).GetCursor();
			client.UpdateCheckPoint(project, logStore, consumerGroup, shardId, cursor);
			break;
		}
	}
}
