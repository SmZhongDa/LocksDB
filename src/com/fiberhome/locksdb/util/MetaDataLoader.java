package com.fiberhome.locksdb.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaDataLoader {

	private Logger logger = LoggerFactory.getLogger(MetaDataLoader.class);
	public static HashMap<String, HashMap<Integer, String>> INDEXMAP = new HashMap<String, HashMap<Integer, String>>();
	public static HashMap<String, Integer> CAPTURETIMEMAP = new HashMap<String, Integer>();
	public static HashMap<String, HashMap<String, Integer>> COLUMNMAP = new HashMap<String, HashMap<String, Integer>>();
	private final String url = ConfigLoader.CONFIGMAP.get("url");
	private final String user = ConfigLoader.CONFIGMAP.get("user");
	private final String passwd = ConfigLoader.CONFIGMAP.get("passwd");

	public void load() {
		logger.info("loading metadata ......");
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			logger.error(e.toString());
			System.exit(1);
		}
		String sql = "SELECT T2.TEMPLATE_NAME,T1.RN FROM (SELECT TEMPLATE_ID,ENAME,ROW_NUMBER() OVER(PARTITION BY TEMPLATE_ID ORDER BY FIELD_ID) RN FROM BASE_FIELD_INFO) T1,BASE_TEMPLATE_INFO T2 WHERE T1.TEMPLATE_ID = T2.TEMPLATE_ID AND T2.DIR_ID = 2 AND T1.ENAME = 'CAPTURE_TIME'";
		try (Connection conn = DriverManager.getConnection(url, user, passwd); PreparedStatement preStatement = conn.prepareStatement(sql); ResultSet rs = preStatement.executeQuery();) {
			while (rs.next()) {
				if (rs.getString(1).matches(".*_.*")) {
					logger.error("illegal protocol : " + rs.getString(1));
				} else
					CAPTURETIMEMAP.put(rs.getString(1).toUpperCase(), rs.getInt(2));
			}
			sql = "SELECT T2.TEMPLATE_NAME, T1.RN, T1.ENAME FROM (SELECT TEMPLATE_ID, ENAME, ISINDEX,ISFILTER, ROW_NUMBER() OVER(PARTITION BY TEMPLATE_ID ORDER BY FIELD_ID) RN FROM BASE_FIELD_INFO) T1, BASE_TEMPLATE_INFO T2 WHERE T1.TEMPLATE_ID = T2.TEMPLATE_ID AND T2.DIR_ID = 2 AND (ISINDEX = 1 or ISFILTER=1)";
			try (PreparedStatement p = conn.prepareStatement(sql); ResultSet r = p.executeQuery();) {
				while (r.next()) {
					if (CAPTURETIMEMAP.containsKey(r.getString(1).toUpperCase())) {
						if (INDEXMAP.containsKey(r.getString(1).toUpperCase())) {
							HashMap<Integer, String> map = INDEXMAP.get(r.getString(1).toUpperCase());
							map.put(r.getInt(2), r.getString(3).toUpperCase());
						} else {
							HashMap<Integer, String> map = new HashMap<Integer, String>();
							map.put(r.getInt(2), r.getString(3).toUpperCase());
							if (ConfigLoader.CONFIGMAP.get("dimension").equals("1")) {
								for (Integer key : ConfigLoader.DIMMAP.keySet()) {
									map.put(key + 1, ConfigLoader.DIMMAP.get(key).name);
								}
							}
							INDEXMAP.put(r.getString(1).toUpperCase(), map);
						}
					}
				}
			}
			sql = "SELECT T2.TEMPLATE_NAME, T1.ENAME, T1.RN FROM (SELECT TEMPLATE_ID, ENAME, ROW_NUMBER() OVER(PARTITION BY TEMPLATE_ID ORDER BY FIELD_ID) RN FROM BASE_FIELD_INFO) T1, BASE_TEMPLATE_INFO T2 WHERE T1.TEMPLATE_ID = T2.TEMPLATE_ID AND T2.DIR_ID = 2";
			try (PreparedStatement p = conn.prepareStatement(sql); ResultSet r = p.executeQuery();) {
				while (r.next()) {
					if (CAPTURETIMEMAP.containsKey(r.getString(1).toUpperCase())) {
						if (COLUMNMAP.containsKey(r.getString(1).toUpperCase())) {
							HashMap<String, Integer> map = COLUMNMAP.get(r.getString(1).toUpperCase());
							map.put(r.getString(2).toUpperCase(), r.getInt(3));
						} else {
							HashMap<String, Integer> map = new HashMap<String, Integer>();
							map.put(r.getString(2).toUpperCase(), r.getInt(3));
							COLUMNMAP.put(r.getString(1).toUpperCase(), map);
						}
					}
				}
			}
		} catch (SQLException e) {
			logger.error(e.toString());
			System.exit(1);
		}
		logger.info("load metadata successfully");
		logger.debug("INDEXMAP : {}", INDEXMAP);
		logger.debug("CAPTURETIMEMAP : {}", CAPTURETIMEMAP);
		logger.debug("COLUMNMAP : {}", COLUMNMAP);
	}
}
