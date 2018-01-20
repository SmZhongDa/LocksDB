package com.fiberhome.locksdb.loader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.facet.FacetField;

import com.fiberhome.locksdb.index.Lucene;
import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.ConfigLoader;
import com.fiberhome.locksdb.util.DimConfig;
import com.fiberhome.locksdb.util.LocksUtil;

public class DimensionLoader extends LuceneLoader {

	private final HashMap<Integer, DimConfig> dimensionMap;

	DimensionLoader(RocksLoader rocksLoader, LinkedBlockingQueue<LineInfo<String, Long, LineArray>> successData, LinkedBlockingQueue<String> errorData) {
		super(rocksLoader, successData, errorData);
		dimensionMap = ConfigLoader.DIMMAP;
	}

	private void insert(Document doc, DimConfig dimConfig, String string) {
		if (null != dimConfig.offset) {
			int j = 0;
			LinkedList<String> list = new LinkedList<String>();
			for (int i : dimConfig.offset) {
				if (string.length() > i) {
					String __s = string.substring(j, i);
					if (!__s.equals(""))
						list.add(__s);
					j = i;
				}
			}
			String __s = string.substring(j);
			if (!__s.equals(""))
				list.add(__s);
			if (!list.isEmpty()) {
				doc.add(new FacetField(dimConfig.name, list.toArray(new String[0])));
			}
		} else
			doc.add(new FacetField(dimConfig.name, string));
	}

	void load() {
		LineInfo<String, Long, LineArray> lineInfo;
		Document doc = new Document();
		HashMap<String, Integer> commitMap = new HashMap<String, Integer>();
		while ((lineInfo = successData.poll()) != null) {
			String tableName = lineInfo.tableName;
			String[] strs = lineInfo.value.array;
			int captureTimeIndex = captureTimeMap.get(tableName);
			doc.add(new Field(Config.CAPTURETIME, strs[captureTimeIndex - 1].getBytes(Config.DEFAULTCHARSET), timeType));
			HashMap<Integer, String> map = indexMap.get(tableName);
			for (int _i = 0; _i < strs.length; _i++) {
				if (map.containsKey(_i + 1)) {
					doc.add(new Field(map.get(_i + 1), strs[_i].getBytes(Config.DEFAULTCHARSET), fieldType));
				}
				if (dimensionMap.containsKey(_i)) {
					DimConfig dimConfig = dimensionMap.get(_i);
					if (dimConfig.separator.isEmpty()) {
						insert(doc, dimConfig, strs[_i]);
					} else {
						for (String _s : strs[_i].split(dimConfig.separator, -1)) {
							if (!_s.isEmpty())
								insert(doc, dimConfig, _s);
						}
					}
				}
			}
			doc.add(new LongPoint(Config._ID, lineInfo.locksID));
			doc.add(new StoredField(Config._ID, lineInfo.locksID));
			long captureTime = Long.parseLong(strs[captureTimeIndex - 1]);
			String time = LocksUtil.secondsToString(sdf, captureTime);
			String name = tableName + Config.SEPARATOR + time;
			if (!commitMap.containsKey(name))
				commitMap.put(name, 0);
			try {
				indexWriterMap.get(name).writer.addDocument(ConfigLoader.CONFIG.build(Lucene.TAXOWRITERMAP.get(name).writer, doc));
				++counter;
				int _i = commitMap.get(name);
				commitMap.put(name, ++_i);
				if (counter % 100000 == 0)
					commit(commitMap);
			} catch (IOException e) {
				logger.error(e.toString());
				logger.error("error data :\n{}\n{}", tableName, lineInfo.value);
				StringBuilder sb = new StringBuilder();
				putErrorData(LocksUtil.stringAppend(sb, tableName, "\n", e, "\n", lineInfo.value));
			}
			doc.clear();
		}
		if (!commitMap.isEmpty())
			commit(commitMap);
	}

	@Override
	void commit(HashMap<String, Integer> commitMap) {
		StringBuilder sb = new StringBuilder();
		sb.append("Lucene commit : [ ");
		Iterator<String> it = commitMap.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			try {
				indexWriterMap.get(key).writer.commit();
				Lucene.TAXOWRITERMAP.get(key).writer.commit();
				sb.append(key);
				sb.append(" : ");
				sb.append(commitMap.get(key));
				if (it.hasNext())
					sb.append(" , ");
				it.remove();
			} catch (IOException e) {
				logger.error(e.toString());
			}
		}
		sb.append(" ]");
		logger.info(sb.toString());
	}

	@Override
	public void run() {
		try {
			while (!rocksLoader.isDone()) {
				load();
			}
			load();
		} finally {
			isDone = true;
		}
	}

}
