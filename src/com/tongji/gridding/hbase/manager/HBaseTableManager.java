package com.tongji.gridding.hbase.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

import com.tongji.gridding.datamodel.HBaseTableData;

public class HBaseTableManager {
	private HTablePool hTablePool = null;
	private HBaseConfigurationManager configManager = null;
	
	public HBaseTableManager(HBaseConfigurationManager configManager) {
		this.configManager = configManager;
		hTablePool = new HTablePool(configManager.getHbaseConf(), 100);
	}
	
	/**
	 * HTableInterface.close() after using the HTable
	 * @param tableName
	 * @return
	 */
	public HTableInterface getHTable(String tableName) {
		return hTablePool.getTable(tableName);
	}
	
	public List<String> getColumnFamilies(HTableInterface table) {
		List<String> colFamilies = new ArrayList<String>();
		try {
			Set<byte[]> familySet = table.getTableDescriptor()
					.getFamiliesKeys();
			for (byte[] family : familySet) {
				 colFamilies.add(Bytes.toString(family));
			}

		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		return colFamilies;
	}
	
	/**
	 * Get <code>HBaseTableData</code>
	 * @param tableName
	 * @return table no exists, null; else HBaseTableData
	 */
	public HBaseTableData getTableData(String tableName) {
		HBaseTableData tableData = new HBaseTableData();
		try{
			if(!configManager.getHbaseAdmin().tableExists(tableName)) {
				return null;
			}
			
			HTableInterface hTableInterface = getHTable(tableName);
			tableData.setColumnFamilies(getColumnFamilies(hTableInterface));
			tableData.setTableName(tableName);
			hTableInterface.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
		return tableData;
		
	}


	public HTablePool gethTablePool() {
		return hTablePool;
	}

	public HBaseConfigurationManager getConfigManager() {
		return configManager;
	}
}
