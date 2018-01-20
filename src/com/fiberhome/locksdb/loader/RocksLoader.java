package com.fiberhome.locksdb.loader;

import java.text.ParseException;
import java.time.LocalTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import com.fiberhome.locksdb.data.DBInfo;
import com.fiberhome.locksdb.data.Rocks;
import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.LocksUtil;

public class RocksLoader extends LocksLoader implements Runnable {

	private final FileReader reader;
	public final LinkedBlockingQueue<LineInfo<String, Long, LineArray>> successData = new LinkedBlockingQueue<LineInfo<String, Long, LineArray>>(100000);
	private final ConcurrentHashMap<String, DBInfo> dbInfos;

	RocksLoader(FileReader reader, LinkedBlockingQueue<String> errorData) {
		super(errorData);
		this.reader = reader;
		this.dbInfos = Rocks.DBINFOS;
	}

	private void load(WriteOptions wops) {
		for (String key : reader.data.keySet()) {
			LinkedBlockingQueue<String> data = reader.data.get(key);
			if(data.isEmpty()){
				continue;
			}
			String line;
			while ((line = data.poll()) != null) {
				String[] strs = line.split("\t", -1);
				int captureTimeIndex = captureTimeMap.get(key);
				if (strs.length < captureTimeIndex) {
					StringBuilder sb = new StringBuilder();
					putErrorData(LocksUtil.stringAppend(sb, key, "\n", "capture time error", "\n", line));
					continue;
				}
				long captureTime;
				try {
					captureTime = Long.parseLong(strs[captureTimeIndex - 1]);
					long today = LocksUtil.stringToSeconds(sdf, LocksUtil.secondsToString(sdf, System.currentTimeMillis() / 1000));
					if (!(today + 86400 * 2 > captureTime && captureTime >= today - 86400 * (interval - 1))) {
						continue;
					}
				} catch (NumberFormatException | ParseException e) {
					StringBuilder sb = new StringBuilder();
					putErrorData(LocksUtil.stringAppend(sb, key, "\n", e, "\n", line));
					continue;
				}

				long locksID = (100000+LocalTime.now().toSecondOfDay())*10000000L+LocksUtil.nextValue();
				String time = LocksUtil.secondsToString(sdf, captureTime);

				try {
					DBInfo info = dbInfos.get(time);
					info.db.put(info.columnFamilyInfos.get(Config.LCOKSTABLE).columnFamilyHandle, wops,  LocksUtil.longToBytes(locksID), line.getBytes(Config.DEFAULTCHARSET));
					++counter;
					successData.put(new LineInfo<String, Long, LineArray>(key, locksID, new LineArray(strs)));
				} catch (RocksDBException e) {
					StringBuilder sb = new StringBuilder();
					putErrorData(LocksUtil.stringAppend(sb, key, "\n", e, "\n", line));
				} catch (InterruptedException e) {
					logger.error(e.toString());
				}
			}
		}
	}

	@Override
	public void run() {
		try {
			WriteOptions wops = new WriteOptions();
			while (!reader.isDone()) {
				load(wops);
			}
			load(wops);
			wops.close();
		} finally {
			isDone = true;
		}
	}

}
