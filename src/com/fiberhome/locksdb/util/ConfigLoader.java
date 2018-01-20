package com.fiberhome.locksdb.util;

import java.io.IOException;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.lucene.facet.FacetsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ConfigLoader {

	private Logger logger = LoggerFactory.getLogger(ConfigLoader.class);
	private final String configPath = System.getProperty("configPath");
	public static HashMap<String, String> CONFIGMAP = new HashMap<String, String>();
	public static HashMap<String, String> PROTOCOLTOIDMAP = new HashMap<String, String>();
	public static HashMap<String, String> IDTOPROTOCOLMAP = new HashMap<String, String>();
	private String dataPath;
	private String indexPath;
	private String url;
	private String user;
	private String passwd;
	private String dataSource;
	private String WALPath;
	private Integer interval;
	private Integer period;
	private Integer sleep;
	private Integer release;
	private Integer reloader;
	private Integer taxoRelease;
	private Integer taxoReloader;
	private Integer port;
	private String dimension;
	private String jettyPath;
	private String lokiIp;
	public static volatile HashMap<Integer, DimConfig> DIMMAP = new HashMap<Integer, DimConfig>();
	public final static FacetsConfig CONFIG = new FacetsConfig();

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[ dataPath : ");
		sb.append(dataPath);
		sb.append(" , indexPath : ");
		sb.append(indexPath);
		sb.append(" , url : ");
		sb.append(url);
		sb.append(" , user : ");
		sb.append(user);
		sb.append(" , passwd : ");
		sb.append(passwd);
		sb.append(" , dataSource : ");
		sb.append(dataSource);
		sb.append(" , WALPath : ");
		sb.append(WALPath);
		sb.append(" , interval : ");
		sb.append(interval);
		sb.append(" , period : ");
		sb.append(period);
		sb.append(" , reloader : ");
		sb.append(reloader);
		sb.append(" , release : ");
		sb.append(release);
		sb.append(" , taxoReloader : ");
		sb.append(taxoReloader);
		sb.append(" , taxoRelease : ");
		sb.append(taxoRelease);
		sb.append(" , port : ");
		sb.append(port);
		sb.append(" , dimension : ");
		sb.append(dimension);
		sb.append(" , dimMap : ");
		sb.append(DIMMAP);
		sb.append(" ]");
		return sb.toString();
	}

	/**
	 * @return the release
	 */
	public Integer getRelease() {
		return release;
	}
	
	
	
	public String getJettyPath() {
		return jettyPath;
	}
	
	public String getLokiIp() {
		return lokiIp;
	}

	/**
	 * @return the reloader
	 */
	public Integer getReloader() {
		return reloader;
	}

	/**
	 * @return the taxoRelease
	 */
	public Integer getTaxoRelease() {
		return taxoRelease;
	}

	/**
	 * @return the taxoReloader
	 */
	public Integer getTaxoReloader() {
		return taxoReloader;
	}

	/**
	 * @return the dimension
	 */
	public String getDimension() {
		return dimension;
	}

	/**
	 * @return the sleep
	 */
	public Integer getSleep() {
		return sleep;
	}

	/**
	 * @return the period
	 */
	public Integer getPeriod() {
		return period;
	}

	/**
	 * @return the interval
	 */
	public Integer getInterval() {
		return interval;
	}

	/**
	 * @return the wALPath
	 */
	public String getWALPath() {
		return WALPath;
	}

	/**
	 * @return the dataSource
	 */
	public String getDataSource() {
		return dataSource;
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @return the passwd
	 */
	public String getPasswd() {
		return passwd;
	}

	/**
	 * @return the dataPath
	 */
	public String getDataPath() {
		return dataPath;
	}

	/**
	 * @return the indexPath
	 */
	public String getIndexPath() {
		return indexPath;
	}

	/**
	 * @return the port
	 */
	public Integer getPort() {
		return port;
	}

	public ConfigLoader() {
		try {
			readConf();
		} catch (ParserConfigurationException | SAXException | IOException e) {
			logger.error(e.toString());
			System.exit(1);
		}
		logger.debug(this.toString());
	}

	public void readConf() throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(configPath);
		NodeList list = doc.getElementsByTagName("locksDB");
		for (int i = 0; i < list.getLength(); i++) {
			Node field = list.item(i);
			for (Node node = field.getFirstChild(); node != null; node = node.getNextSibling()) {
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					if (node.getNodeName().equals("dataPath")) {
						dataPath = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						CONFIGMAP.put("dataPath", dataPath);
					}
					if (node.getNodeName().equals("indexPath")) {
						indexPath = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						CONFIGMAP.put("indexPath", indexPath);
					}
					if (node.getNodeName().equals("url")) {
						url = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						CONFIGMAP.put("url", url);
					}
					if (node.getNodeName().equals("user")) {
						user = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						CONFIGMAP.put("user", user);
					}
					if (node.getNodeName().equals("passwd")) {
						passwd = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						CONFIGMAP.put("passwd", passwd);
					}
					if (node.getNodeName().equals("dataSource")) {
						dataSource = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						CONFIGMAP.put("dataSource", dataSource);
					}
					if (node.getNodeName().equals("WALPath")) {
						WALPath = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						CONFIGMAP.put("WALPath", WALPath);
					}
					if (node.getNodeName().equals("interval")) {
						String s = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						interval = Integer.parseInt(s);
						CONFIGMAP.put("interval", s);
					}
					if (node.getNodeName().equals("dimension")) {
						dimension = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						CONFIGMAP.put("dimension", dimension);
					}
				}
			}

		}
		
		
		list = doc.getElementsByTagName("jetty");
		for (int i = 0; i < list.getLength(); i++) {
			Node field = list.item(i);
			for (Node node = field.getFirstChild(); node != null; node = node.getNextSibling()) {
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					if (node.getNodeName().equals("onlinepath")) {
						String path  = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						jettyPath = LocksUtil.getPath(path);
						CONFIGMAP.put("jettyPath", jettyPath);
					}
					
					if (node.getNodeName().equals("lokiip")) {
						lokiIp = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						CONFIGMAP.put("lokiIp", lokiIp);
					}
				}
			}

		}
		
		
		
		list = doc.getElementsByTagName("core");
		for (int i = 0; i < list.getLength(); i++) {
			Node field = list.item(i);
			for (Node node = field.getFirstChild(); node != null; node = node.getNextSibling()) {
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					if (node.getNodeName().equals("period")) {
						String s = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						period = Integer.parseInt(s);
						CONFIGMAP.put("period", s);
					}
					if (node.getNodeName().equals("sleep")) {
						String s = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						sleep = Integer.parseInt(s);
						CONFIGMAP.put("sleep", s);
					}
					if (node.getNodeName().equals("reloader")) {
						String s = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						reloader = Integer.parseInt(s);
						CONFIGMAP.put("reloader", s);
					}
					if (node.getNodeName().equals("release")) {
						String s = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						release = Integer.parseInt(s);
						CONFIGMAP.put("release", s);
					}
					if (node.getNodeName().equals("taxoReloader")) {
						String s = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						taxoReloader = Integer.parseInt(s);
						CONFIGMAP.put("taxoReloader", s);
					}
					if (node.getNodeName().equals("taxoRelease")) {
						String s = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						taxoRelease = Integer.parseInt(s);
						CONFIGMAP.put("taxoRelease", s);
					}
					if (node.getNodeName().equals("port")) {
						String s = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						port = Integer.parseInt(s);
						CONFIGMAP.put("port", s);
					}
				}
			}

		}
		list = doc.getElementsByTagName("dimension");
		for (int i = 0; i < list.getLength(); i++) {
			Node field = list.item(i);
			for (Node node = field.getFirstChild(); node != null; node = node.getNextSibling()) {
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					if (node.getNodeName().equals("field")) {
						String s = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						for (String _s : s.split(",", -1)) {
							String[] arr = _s.split(":", -1);
							if (!arr[3].isEmpty())
								CONFIG.setMultiValued(arr[0], true);
							int[] offset = null;
							if (!arr[2].isEmpty()) {
								CONFIG.setHierarchical(arr[0], true);
								String[] __a = arr[2].split("-", -1);
								offset = new int[__a.length];
								for (int _i = 0; _i < __a.length; _i++) {
									offset[_i] = Integer.parseInt(__a[_i]);
								}
							}
							DimConfig dimConfig = new DimConfig(arr[0], offset, arr[3]);
							DIMMAP.put(Integer.parseInt(arr[1]), dimConfig);
						}
					}
				}
			}

		}

		list = doc.getElementsByTagName("protocol");
		for (int i = 0; i < list.getLength(); i++) {
			Node field = list.item(i);
			for (Node node = field.getFirstChild(); node != null; node = node.getNextSibling()) {
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					if (node.getNodeName().equals("protocoltoid")) {
						String s = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						for (String _s : s.split(",", -1)) {
							String[] arr = _s.split(":", -1);
							if (arr.length == 2) {
								PROTOCOLTOIDMAP.put(arr[0], arr[1]);
							} else {
								logger.error("protocol:id does not complete.");
								System.exit(1);
							}

						}
					}
					if (node.getNodeName().equals("idtoprotocol")) {
						String s = LocksUtil.replaceBlank(node.getFirstChild().getNodeValue());
						for (String _s : s.split(",", -1)) {
							String[] arr = _s.split(":", -1);
							if (arr.length == 2) {
								IDTOPROTOCOLMAP.put(arr[0], arr[1]);
							} else {
								logger.error("id:protocol does not complete.");
								System.exit(1);
							}

						}
					}

				}
			}

		}

	}

}
