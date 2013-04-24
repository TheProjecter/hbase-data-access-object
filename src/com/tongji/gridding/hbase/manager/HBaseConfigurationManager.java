package com.tongji.gridding.hbase.manager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.tongji.gridding.datamodel.HBaseConfigurationData;

public class HBaseConfigurationManager {

	private static Configuration hbaseConf = null;
	
	private static HBaseAdmin hbaseAdmin = null;
	
	public HBaseConfigurationManager(HBaseConfigurationData configData)
	{
		if(hbaseConf != null && hbaseAdmin != null) {
			return ;
		}
		initConnection(configData);
	}
	
	public HBaseConfigurationManager(Configuration config) {
		if(hbaseConf != null && hbaseAdmin != null) {
			return ;
		}
		try {
			hbaseAdmin = new HBaseAdmin(config);
			hbaseConf = config;
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	private void initConnection(HBaseConfigurationData configData) {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", configData.getHbaseZookeeperQuorum());
		config.set("hbase.zookeeper.property.clientPort",configData.getHbaseZookeeperPropertyClientPort());
		config.set("hbase.master", configData.getHbaseMaster());

		try {
			hbaseAdmin = new HBaseAdmin(config);
			hbaseConf = config;
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	public Configuration getHbaseConf() {
		return hbaseConf;
	}
	public HBaseAdmin getHbaseAdmin() {
		return hbaseAdmin;
		
		
	}	
}
