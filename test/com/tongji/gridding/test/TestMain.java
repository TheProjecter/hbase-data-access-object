package com.tongji.gridding.test;

import java.io.UnsupportedEncodingException;

import com.tongji.gridding.datamodel.HBaseConfigurationData;
import com.tongji.gridding.hbase.HBaseDao;


public class TestMain {

	/**
	 * @param args
	 * @throws UnsupportedEncodingException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException {
		HBaseConfigurationData configData = new HBaseConfigurationData();
		configData.setHbaseZookeeperPropertyClientPort("2181");
		configData.setHbaseMaster("namenode01:60000");
		configData.setHbaseZookeeperQuorum("namenode01,datanode04,datanode06");
		
		HBaseDao dao = new HBaseDao(configData);
		
		dao.insert("PressureTest3", "abcd", "test", "1", "testa");
		dao.insert("PressureTest3", "abcd", "test", "2", "testb");
		dao.insert("PressureTest3", "abcd", "test", "3", "testc");
	
		
		System.out.println(dao.getCFSize("document_index_data", "同济大学", "document_index"));
		
//		
//		String a = dao.query("PressureTest3", "abcd", "test", "1");
//		System.out.println(a);
//	
//		dao.deleteQualifier("PressureTest3", "abcd", "test", "1");
//		
//		String b = dao.query("PressureTest3", "abcd", "test", "1");
//		System.out.println(b);
//		
//		String c = dao.query("PressureTest3", "abcd", "test", "2");
//		System.out.println(c);
//		
		
		
		
		//dao.createTable("PressureTest", new String[]{"test"});
		
		
//		for(int i=0; i< 500000; ++i) {
//			Map<String, String> m = new HashMap<String, String>();
//			m.put("1", "-------------" + i + "--------------");
//			dao.insert("PressureTest", ""+i, "test", m);
//			System.out.println(i);
//		}
		
		//dao.insert("PressureTest", "1", "test", m);
//		System.out.println(dao.queryMapString("words_relevancy_big_data", "哲学", "relevancy"));
//		System.out.println("1");
	
//		
//		boolean t = dao.createTable("test", new String[]{"test"});
//		if (t) {
//			System.out.println("create table success");
//		}
//		List<String> l = new ArrayList<>();
//		l.add("3");
//		l.add("4");
//		Map<String, Map<String, String>> a  = dao.queryScan("test_inverted_index_items", "item_info", "1", "25");
//		List<String> r = new ArrayList<>();
//		r.add("17973109");
//		r.add("17973104");
//		List<String> s = dao.queryList("test_inverted_index_items", r,  "item_info", "url");
//		dao.query("test2", "3993801", "item_info", "url");
//		System.out.println(a.get(0));
//		System.out.println(a.get(1));
//		
//		for (String string : s) {
//			System.out.println(string);
//		}
		
//		Set set = a.entrySet();
//		for (Iterator it = set.iterator(); it.hasNext(); ) {
//            Map.Entry entry = (Entry) it.next();
//            System.out.println(entry.getKey());
//		}
		
		
		
		
	}

}
