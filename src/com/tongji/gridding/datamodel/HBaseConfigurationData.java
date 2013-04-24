package com.tongji.gridding.datamodel;

public class HBaseConfigurationData {
	private String hbaseZookeeperQuorum;
	private String hbaseZookeeperPropertyClientPort;
	private String hbaseMaster;

	public HBaseConfigurationData() {
		
	}
	
	public HBaseConfigurationData(String hbaseZookeeperQuorum,
			String hbaseZookeeperPropertyClientPort, String hbaseMaster) {
		super();
		this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
		this.hbaseZookeeperPropertyClientPort = hbaseZookeeperPropertyClientPort;
		this.hbaseMaster = hbaseMaster;
	}
	
	public String getHbaseZookeeperQuorum() {
		return hbaseZookeeperQuorum;
	}

	public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
		this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
	}

	public String getHbaseZookeeperPropertyClientPort() {
		return hbaseZookeeperPropertyClientPort;
	}

	public void setHbaseZookeeperPropertyClientPort(
			String hbaseZookeeperPropertyClientPort) {
		this.hbaseZookeeperPropertyClientPort = hbaseZookeeperPropertyClientPort;
	}

	public String getHbaseMaster() {
		return hbaseMaster;
	}

	public void setHbaseMaster(String hbaseMaster) {
		this.hbaseMaster = hbaseMaster;
	}

	
	
	
}
