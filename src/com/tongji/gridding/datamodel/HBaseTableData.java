package com.tongji.gridding.datamodel;

import java.util.List;

public class HBaseTableData {
	private String tableName;
	private List<String> columnFamilies;
	
	public HBaseTableData() {
		
	}
	
	public HBaseTableData(String tableName, List<String> columnFamilies) {
		super();
		this.tableName = tableName;
		this.columnFamilies = columnFamilies;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public List<String> getColumnFamilies() {
		return columnFamilies;
	}
	
	public void setColumnFamilies(List<String> columnFamilies) {
		this.columnFamilies = columnFamilies;
	}
}
