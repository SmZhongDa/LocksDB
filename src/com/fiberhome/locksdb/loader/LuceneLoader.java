package com.fiberhome.locksdb.loader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;

import com.fiberhome.locksdb.index.Lucene;
import com.fiberhome.locksdb.index.LuceneWriter;
import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.LocksUtil;
import com.fiberhome.locksdb.util.MetaDataLoader;

public class LuceneLoader extends LocksLoader implements Runnable {

	final RocksLoader rocksLoader;
	final LinkedBlockingQueue<LineInfo<String, Long, LineArray>> successData;
	final FieldType fieldType;
	final FieldType timeType;
	// final FieldType keyType;
	final HashMap<String, HashMap<Integer, String>> indexMap;
	final ConcurrentHashMap<String, LuceneWriter> indexWriterMap;

	LuceneLoader(RocksLoader rocksLoader, LinkedBlockingQueue<LineInfo<String, Long, LineArray>> successData, LinkedBlockingQueue<String> errorData) {
		super(errorData);
		this.rocksLoader = rocksLoader;
		this.successData = successData;
		fieldType = new FieldType();
		fieldType.setDocValuesType(DocValuesType.NONE);
		fieldType.setIndexOptions(IndexOptions.DOCS);
		fieldType.setStored(false);
		fieldType.setTokenized(false);
		fieldType.setOmitNorms(true);
		fieldType.freeze();
		timeType = new FieldType();
		timeType.setDocValuesType(DocValuesType.SORTED);
		timeType.setIndexOptions(IndexOptions.DOCS);
		timeType.setStored(false);
		timeType.setTokenized(false);
		timeType.setOmitNorms(true);
		timeType.freeze();
		// keyType = new FieldType();
		// keyType.setDocValuesType(DocValuesType.NONE);
		// keyType.setIndexOptions(IndexOptions.NONE);
		// keyType.setStored(true);
		// keyType.setTokenized(false);
		// keyType.setOmitNorms(true);
		// keyType.freeze();
		this.indexMap = MetaDataLoader.INDEXMAP;
		this.indexWriterMap = Lucene.INDEXWRITERMAP;
	}

	private void load() {
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
					byte[] bt = strs[_i].getBytes(Config.DEFAULTCHARSET);
					if(bt.length > 32760){
						bt = strs[_i].substring(0, 256).getBytes(Config.DEFAULTCHARSET);
					}
					doc.add(new Field(map.get(_i + 1), bt, fieldType));
				}
			}
			doc.add(new LongPoint(Config._ID, lineInfo.locksID));
			doc.add(new StoredField(Config._ID, lineInfo.locksID));
			// doc.add(new Field(Config._ID, locksID, keyType));
			long captureTime = Long.parseLong(strs[captureTimeIndex - 1]);
			String time = LocksUtil.secondsToString(sdf, captureTime);
			String name = tableName + Config.SEPARATOR + time;
			if (!commitMap.containsKey(name))
				commitMap.put(name, 0);
			try {
				indexWriterMap.get(name).writer.addDocument(doc);
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

	void commit(HashMap<String, Integer> commitMap) {
		StringBuilder sb = new StringBuilder();
		sb.append("Lucene commit : [ ");
		Iterator<String> it = commitMap.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			try {
				indexWriterMap.get(key).writer.commit();
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
