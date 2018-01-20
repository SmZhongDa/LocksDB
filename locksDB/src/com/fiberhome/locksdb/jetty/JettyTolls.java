package com.fiberhome.locksdb.jetty;

import java.io.IOException;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JettyTolls {
	private static Logger logger = LoggerFactory.getLogger(JettyTolls.class);
	public static String url = JettyStart.rc.getUrl();
	public static String user = JettyStart.rc.getUser();
	public static String passwd = JettyStart.rc.getPasswd();
	public static java.sql.Connection conn = null;

	public static List<String> getlocksNode() throws SQLException {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection(url, user, passwd);
		} catch (Exception e) {
			logger.error("jettyTolls procedure failed : {}", e.toString());
			System.exit(1);
		}
		List<String> list = new ArrayList<String>();
		String sql = "SELECT ipaddress FROM DBN_DATABASES WHERE STATUS = 1";
		PreparedStatement preStatement = conn.prepareStatement(sql);
		ResultSet rs = preStatement.executeQuery();
		while (rs.next()) {
			list.add(rs.getString(1));
		}
		return list;
	}

	public static Session login(String ip) throws IOException {
		Connection conn = new Connection(ip);
		conn.connect();
		conn.authenticateWithPassword("locksuser", "123456");
		Session session = conn.openSession();
		return session;

	}

	public static void init(HashMap<String, Session> initMap)
			throws SQLException, IOException {
		List<String> list = getlocksNode();
		for (String ip : list) {
			Session session = login(ip);
			initMap.put(ip, session);
		}

	}



	public static List<String> parserLine(String[] line, int flag) {
		List<String> list = new ArrayList<String>();
		if (line.length < 10)
			flag = 0;
		for (int i = line.length - 1; i >= 0; i--) {
			if (line[i].isEmpty())
				continue;
			list.add(line[i].trim());
		}

		if (flag == 1) {
			if (list.size() > 10) {
				while (list.size() > 10)
					list.remove(list.size() - 1);
			} else if (list.size() < 10) {
				while (list.size() < 10)
					list.add("0");
			}
		}
		return list;
	}

}
