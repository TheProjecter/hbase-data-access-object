package com.tongji.gridding.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.tongji.gridding.datamodel.HBaseConfigurationData;
import com.tongji.gridding.hbase.manager.HBaseConfigurationManager;
import com.tongji.gridding.hbase.manager.HBaseTableManager;

public class HBaseDao {
	private HBaseConfigurationManager configManager = null;
	private HBaseTableManager tableManager = null;

	public HBaseDao(HBaseConfigurationData configData) {
		configManager = new HBaseConfigurationManager(configData);
		tableManager = new HBaseTableManager(configManager);
	}
	
	public HBaseDao(Configuration config) {
		configManager = new HBaseConfigurationManager(config);
		tableManager = new HBaseTableManager(configManager);
	}

	
	public boolean checkTable(String tableName, String familyName) {
		if (tableManager.getTableData(tableName) == null
				|| !tableManager.getTableData(tableName).getColumnFamilies()
						.contains(familyName)) {
			System.err.println("Table or Family not exists.");
			return false;
		}
		return true;
	}
	
	/**
	 * Retrieve one qualifier values with rowkey and qualifier
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param familyName
	 * @param qualifier
	 * @return If none, return null;
	 */
	public String query(String tableName, String rowkey, String familyName,
			String qualifier) {	
		String value = null;
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		try {
			Get scan = new Get(Bytes.toBytes(rowkey));
			scan.setMaxVersions();
			Result r = hTableInterface.get(scan);
			byte[] b = r.getValue(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
			if (b != null) {
				value = Bytes.toString(b);
			}
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		return value;
	}

	/**
	 * Retrieve qualifiers TO values with rowkey
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param familyName
	 * @return Map<String, String>
	 */
	public Map<String, String> queryMapString(String tableName, String rowkey,
			String familyName) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Map<String, String> familyValues = new HashMap<String, String>();
		try {
			Get scan = new Get(Bytes.toBytes(rowkey));
			scan.setMaxVersions();
			Result r = hTableInterface.get(scan);
			Map<byte[], byte[]> tempFamilyValues = r.getFamilyMap(Bytes.toBytes(familyName));

			Set<Map.Entry<byte[], byte[]>> set = tempFamilyValues.entrySet();
			for (Iterator<Map.Entry<byte[], byte[]>> it = set.iterator(); it
					.hasNext();) {
				Map.Entry<byte[], byte[]> entry = (Map.Entry<byte[], byte[]>) it
						.next();
				familyValues.put(Bytes.toString(entry.getKey()),
						Bytes.toString(entry.getValue()));
			}
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		return familyValues;
	}
	
	public int getCFSize(String tableName, String rowkey, String familyName) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		int size = 0;
		try {
			Get scan = new Get(Bytes.toBytes(rowkey));
			scan.setMaxVersions();
			Result r = hTableInterface.get(scan);
			size = r.size();
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		return size;
	}
	
	/**
	 * Retrieve qualifiers TO values with rowkey
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param familyName
	 * @return Map<byte[], byte[]>
	 */
	public Map<byte[], byte[]> queryMapBytes(String tableName, String rowkey,
			String familyName) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Map<byte[], byte[]> familyValues = null;
		try {
			Get scan = new Get(Bytes.toBytes(rowkey));
			scan.setMaxVersions();
			Result r = hTableInterface.get(scan);
			familyValues = r.getFamilyMap(Bytes.toBytes(familyName));
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		return familyValues;
	}
	
	
	public Map<byte[], byte[]> queryMapBytes(String tableName, byte[] rowkey, byte[] familyName) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Map<byte[], byte[]> familyValues = null;
		try {
			Get scan = new Get(rowkey);
			scan.setMaxVersions();
			Result r = hTableInterface.get(scan);
			familyValues = r.getFamilyMap(familyName);
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		return familyValues;
	}
	
	/**
	 * List get results, quicker when data at the same region
	 * @param tableName
	 * @param rowkeys
	 * @param familyName
	 * @return Map<col, value>
	 */
	public List<Map<String, String>> queryList(String tableName, List<String> rowkeys, String familyName) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		List<Map<String, String>> familyValuesList = new ArrayList<Map<String,String>>();
		try {
			List<Get> gets = new ArrayList<Get>();
			for (String rowkey : rowkeys) {
				gets.add(new Get(Bytes.toBytes(rowkey)));
			}
			Result[] results = hTableInterface.get(gets);
			for (Result result : results) {
				if(result==null || result.isEmpty()) {
					familyValuesList.add(null);
					continue;
				}
				
				Map<String, String> familyValues = new HashMap<String, String>();
				Map<byte[], byte[]> tempFamilyValues = result.getFamilyMap(Bytes.toBytes(familyName));
				Set<Map.Entry<byte[], byte[]>> set = tempFamilyValues.entrySet();
				for (Iterator<Map.Entry<byte[], byte[]>> it = set.iterator(); it
						.hasNext();) {
					Map.Entry<byte[], byte[]> entry = (Map.Entry<byte[], byte[]>) it
							.next();
					familyValues.put(Bytes.toString(entry.getKey()),
							Bytes.toString(entry.getValue()));
				}
				familyValuesList.add(familyValues);
			}
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
		return familyValuesList;
	}
	
	/**
	 * List Get, efficient if the data at the same region
	 * Add <code>null</code> to the pos if the rowkey or qualifier does not exist
	 * @param tableName
	 * @param rowkeys
	 * @param familyName
	 * @param qualifier
	 * @return list of qualifier value
	 */
	public List<String> queryList(String tableName, List<String> rowkeys, String familyName, String qualifier) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		List<String> values = new ArrayList<String>();
		try {
			List<Get> gets = new ArrayList<Get>();
			for (String rowkey : rowkeys) {
				gets.add(new Get(Bytes.toBytes(rowkey)));
			}
			Result[] results = hTableInterface.get(gets);
			for (Result result : results) {
				if(result==null || result.isEmpty()) {
					values.add(null);
					continue;
				}
				byte[] b = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
				if(b == null) {
					values.add(null);
					continue;
				}
				values.add(Bytes.toString(b));
			}
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
		return values;
	}
	
	public Map<String,Map<String, String>> queryScan(String tableName, String familyName, String startRow, String stopRow){
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Map<String, Map<String, String>> familyValuesList = new HashMap<String, Map<String,String>>();
		try{
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startRow));
			scan.setStopRow(Bytes.toBytes(stopRow));
			ResultScanner results = hTableInterface.getScanner(scan);
			
			for (Result result : results) {
				Map<String, String> familyValues = new HashMap<String, String>();
				Map<byte[], byte[]> tempFamilyValues = result.getFamilyMap(Bytes.toBytes(familyName));
				Set<Map.Entry<byte[], byte[]>> set = tempFamilyValues.entrySet();
				for (Iterator<Map.Entry<byte[], byte[]>> it = set.iterator(); it
						.hasNext();) {
					Map.Entry<byte[], byte[]> entry = (Map.Entry<byte[], byte[]>) it
							.next();
					familyValues.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
				}
				familyValuesList.put(Bytes.toString(result.getRow()), familyValues);
			}
			results.close();
			hTableInterface.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
		return familyValuesList;
	}

	/**
	 * Insert values into one given family
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param familyName
	 * @param values (String)
	 */
	public void insertMapString(String tableName, String rowkey, String familyName,
			Map<String, String> values) {
		if(values == null) {
			return ;
		}
		
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Put put = new Put(Bytes.toBytes(rowkey));
		Set<Map.Entry<String, String>> set = values.entrySet();
		for (Iterator<Map.Entry<String, String>> it = set.iterator(); it
				.hasNext();) {
			Map.Entry<String, String> entry = (Map.Entry<String, String>) it
					.next();
			put.add(Bytes.toBytes(familyName), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry
					.getValue()));
		}

		try {
			hTableInterface.put(put);
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	
	/**
	 * Insert values into one given family
	 * 
	 * @param tableName
	 * @param rowkey
	 * @param familyName
	 * @param values (byte[])
	 */
	public void insertMapBytes(String tableName, String rowkey, String familyName,
			Map<byte[], byte[]> values) {
		if(values == null) {
			return ;
		}
		
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Put put = new Put(Bytes.toBytes(rowkey));
		Set<Map.Entry<byte[], byte[]>> set = values.entrySet();
		for (Iterator<Map.Entry<byte[], byte[]>> it = set.iterator(); it
				.hasNext();) {
			Map.Entry<byte[], byte[]> entry = (Map.Entry<byte[], byte[]>) it
					.next();
			put.add(Bytes.toBytes(familyName), entry.getKey(), entry.getValue());
		}

		try {
			hTableInterface.put(put);
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	/**
	 * Simple insert data into HBase
	 * @param tableName
	 * @param rowkey
	 * @param familyName
	 * @param qualifier
	 * @param value
	 */
	public void insert(String tableName, String rowkey, String familyName, String qualifier, String value) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Put put = new Put(Bytes.toBytes(rowkey));
		
		put.add(Bytes.toBytes(familyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		try{
			hTableInterface.put(put);
			hTableInterface.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	public void insert(String tableName, byte[] rowkey, byte[] familyName, byte[] qualifier, byte[] value) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Put put = new Put(rowkey);
		
		put.add(familyName, qualifier, value);
		try{
			hTableInterface.put(put);
			hTableInterface.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	
	public void insert2(String tableName, String rowkey, String familyName, String qualifier, String value) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Put put = new Put(rowkey.getBytes());
		
		put.add(familyName.getBytes(), qualifier.getBytes(), value.getBytes());
		try{
			hTableInterface.put(put);
			hTableInterface.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	/**
	 * Given rowkey delete the corresponding row
	 * @param tableName
	 * @param rowkey
	 */
	public void deleteRow(String tableName, String rowkey) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowkey));
		
		try {
			hTableInterface.delete(delete);
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	/**
	 * Given an array of rowkeys, delete them. Better when data at the same region
	 * @param tableName
	 * @param rowkeys
	 */
	public void deleteRows(String tableName, String[] rowkeys) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		List<Delete> list = new ArrayList<Delete>();
		for (String rowkey : rowkeys) {
			Delete d = new Delete(Bytes.toBytes(rowkey));
			list.add(d);
		}
		try {
			hTableInterface.delete(list);
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	public void deleteQualifier(String tableName, String rowkey, String family, String qualifier) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowkey));
		delete.deleteColumns(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		
		try {
			hTableInterface.delete(delete);
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	public void deleteQualifier(String tableName, byte[] rowkey, byte[] family, byte[] qualifier) {
		HTableInterface hTableInterface = tableManager.getHTable(tableName);
		Delete delete = new Delete(rowkey);
		delete.deleteColumns(family, qualifier);
		
		try {
			hTableInterface.delete(delete);
			hTableInterface.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	
	/**
	 * Create table if not exists, else do nothing
	 * 
	 * @param tableName
	 * @param families
	 * @return
	 */
	public boolean createTable(String tableName, String[] families) {
		try {
			HBaseAdmin hBaseAdmin = configManager.getHbaseAdmin();
			if (hBaseAdmin.tableExists(tableName)) {
				System.err.println(tableName + " already exists...");
				return false;
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			for (String family : families) {
				tableDescriptor.addFamily(new HColumnDescriptor(family));
			}
			hBaseAdmin.createTable(tableDescriptor);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Drop table in HBase (DANGEROUS! DO NOT ACCESS, IF NOT NECESSARY)
	 * 
	 * @param tableName
	 */
	public void dropTable(String tableName) {
		try {
			HBaseAdmin hBaseAdmin = configManager.getHbaseAdmin();
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

}
