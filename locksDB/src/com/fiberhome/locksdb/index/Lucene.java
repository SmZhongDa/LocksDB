package com.fiberhome.locksdb.index;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.ConfigLoader;
import com.fiberhome.locksdb.util.LocksUtil;

public class Lucene {

	private static Logger logger = LoggerFactory.getLogger(Lucene.class);
	private final HashMap<String, HashMap<Integer, String>> indexMap;
	private final String path;
	public static final ConcurrentHashMap<String, LuceneWriter> INDEXWRITERMAP = new ConcurrentHashMap<String, LuceneWriter>();
	public static final ConcurrentHashMap<String, LuceneReader> INDEXREADERMAP = new ConcurrentHashMap<String, LuceneReader>();
	public static final ConcurrentHashMap<String, LuceneTaxoWriter> TAXOWRITERMAP = new ConcurrentHashMap<String, LuceneTaxoWriter>();
	public static final ConcurrentHashMap<String, LuceneTaxoReader> TAXOREADERMAP = new ConcurrentHashMap<String, LuceneTaxoReader>();

	public Lucene(HashMap<String, HashMap<Integer, String>> indexMap, String path) {
		this.indexMap = indexMap;
		if (!path.endsWith("/"))
			this.path = path + "/";
		else
			this.path = path;
	}

	public void open() throws IOException {
		logger.info("open Lucene : " + path);
		Set<String> directories = indexMap.keySet();
		List<String> days = LocksUtil.getDays();
		for (String _s : directories) {
			for (String __s : days) {
				Analyzer analyzer = Config.defaultAnalyzer();
				IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
				iwc.setRAMBufferSizeMB(1 << 6);
				String key = _s + Config.SEPARATOR + __s;
				String _d = path + key;
				if (!INDEXWRITERMAP.containsKey(key)) {
					logger.info("open index : " + key);
					Path _p = Paths.get(_d);
					IndexWriter writer = new IndexWriter(FSDirectory.open(_p), iwc);
					INDEXWRITERMAP.put(key, new LuceneWriter(writer, _p));
					INDEXREADERMAP.put(key, new LuceneReader(key, DirectoryReader.open(writer)));
				}
				if (ConfigLoader.CONFIGMAP.get("dimension").equals("1") && !TAXOWRITERMAP.containsKey(key)) {
					String _k = key + Config.SEPARATOR + "TAXO";
					logger.info("open taxo index : " + _k);
					Path _p = Paths.get(path + _k);
					DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(FSDirectory.open(_p), OpenMode.CREATE_OR_APPEND);
					TAXOWRITERMAP.put(key, new LuceneTaxoWriter(taxoWriter, _p));
					TAXOREADERMAP.put(key, new LuceneTaxoReader(key, new DirectoryTaxonomyReader(taxoWriter)));
				}
			}
		}
		logger.info("open Lucene successfully");
	}

	public void reopen(List<String> list) throws IOException {
		logger.info("Lucene maintainer running ......");
		Iterator<String> it = INDEXWRITERMAP.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			String[] strs = key.split(Config.SEPARATOR, -1);
			if (strs.length == 2) {
				String time = strs[1];
				if (!list.contains(time)) {
					logger.info("drop index : " + key);
					INDEXREADERMAP.get(key).reader.close();
					INDEXREADERMAP.remove(key);
					LuceneWriter writer = INDEXWRITERMAP.get(key);
					it.remove();
					writer.writer.deleteAll();
					writer.writer.close();
					LocksUtil.deleteFiles(writer.path);
				}
			}
		}
		it = TAXOWRITERMAP.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			String[] strs = key.split(Config.SEPARATOR, -1);
			if (strs.length == 2) {
				String time = strs[1];
				if (!list.contains(time)) {
					logger.info("drop taxo index : " + key);
					TAXOREADERMAP.get(key).reader.close();
					TAXOREADERMAP.remove(key);
					LuceneTaxoWriter writer = TAXOWRITERMAP.get(key);
					it.remove();
					writer.writer.close();
					LocksUtil.deleteFiles(writer.path);
				}
			}
		}
		open();
		logger.info("maintain Lucene successfully");
	}

	public void close() throws IOException {
		logger.info("close Lucene ......");
		for (LuceneWriter lw : INDEXWRITERMAP.values()) {
			lw.writer.close();
		}
		for (LuceneTaxoWriter ltw : TAXOWRITERMAP.values()) {
			ltw.writer.close();
		}
	}

}
