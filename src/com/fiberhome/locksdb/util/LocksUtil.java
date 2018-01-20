package com.fiberhome.locksdb.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fiberhome.locksdb.query.Pair;

public class LocksUtil {

	private static SimpleDateFormat timeSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private static SimpleDateFormat daySdf = new SimpleDateFormat("yyyyMMdd");
	private static AtomicInteger sequence = new AtomicInteger();

	static {
		timeSdf.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));
		daySdf.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));
	}

	private LocksUtil() {
	}

	private synchronized static String format(SimpleDateFormat sdf, Date date) {
		return sdf.format(date);
	}

	private synchronized static Date parse(SimpleDateFormat sdf, String string) throws ParseException {
		return sdf.parse(string);
	}

	public static LinkedList<String> getPartitions(int index, int offset) {
		LinkedList<String> list = new LinkedList<String>();
		long current = System.currentTimeMillis();
		for (long i = index; i < offset; i++) {
			list.add(format(daySdf, new Date(current - i * 86400000)));
		}
		return list;
	}

	public static LinkedList<String> getPartitions(String partitions) {
		List<String> list = getDays();
		LinkedList<String> partitionList = new LinkedList<String>();
		if (partitions.equals("") || null == partitions)
			partitionList.addAll(list);
		else {
			for (int i = 0; i < partitions.length(); i += 8) {
				String _s = partitions.substring(i, i + 8);
				if (list.contains(_s))
					partitionList.add(_s);
			}
		}
		return partitionList;
	}

	public static LinkedList<String> getPartitions(SimpleDateFormat sdf, int first, int last) {
		LinkedList<String> list = new LinkedList<String>();
		long current = System.currentTimeMillis();
		for (long i = first; i < last; i++) {
			list.add(sdf.format(new Date(current - i * 86400000)));
		}
		return list;
	}

	public static String getDate(long captureTime) {
		SimpleDateFormat sdf = new SimpleDateFormat("MMdd");
		String date = sdf.format(new Date(captureTime * 1000L));
		return date;
	}

	public static String getAllDate(long captureTime) {
		SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMddHHmmss");
		String date = sdf.format(new Date(captureTime * 1000L));
		return date;
	}

	public static String getSecond() {
		Calendar cal = Calendar.getInstance();
		long h = cal.get(Calendar.HOUR_OF_DAY);
		long m = cal.get(Calendar.MINUTE);
		long s = cal.get(Calendar.SECOND);
		long seconds = h * 3600 + m * 60 + s;

		return String.valueOf(seconds);
	}

	public static void sequenceReset() {
		sequence = new AtomicInteger();
	}

	public static String getPfixDate(String id) {
		String date = id.substring(3, 7);
		String _date = "2017" + date;
		return _date;
	}

	public static String getPath(String initPath){
		String path = "";
		if(null != initPath){
			if(initPath.endsWith("/")){
				path = initPath.substring(0,initPath.length());
			}else{
				path = initPath;
			}
		}
		
		return path;

	}
	
	public static String getTime() {
		return format(timeSdf, new Date()) + " : Lucene : ";
	}

	public static String getToday() {
		return format(daySdf, new Date());
	}

	public static List<String> getDays() {
		int interval = Integer.parseInt(ConfigLoader.CONFIGMAP.get("interval"));
		int total = interval + 3;
		List<String> days = new LinkedList<String>();
		long time = System.currentTimeMillis() + 3 * 86400000;
		days.add(format(daySdf, new Date(time)));
		for (int i = 1; i < total; i++) {
			time -= 86400000;
			days.add(format(daySdf, new Date(time)));
		}
		return days;
	}

	public static long nextValue() {
		return sequence.incrementAndGet();
	}

	public static long reverseLong(long l) {
		long result = 0L;
		for (long _l = l; _l != 0; _l /= 10) {
			result = result * 10 + _l % 10;
		}
		return result;
	}

	public static byte[] longToBytes(long _l) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.putLong(0, _l);
		return buffer.array();
	}

	public static long bytesToLong(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
		buffer.put(bytes);
		buffer.flip();
		return buffer.getLong();
	}

	public static String secondsToString(long seconds) {
		return format(daySdf, new Date(seconds * 1000));
	}

	public static String secondsToString(SimpleDateFormat sdf, long seconds) {
		return sdf.format(new Date(seconds * 1000));
	}

	public static long stringToSeconds(String string) throws ParseException {
		return parse(daySdf, string).getTime() / 1000;
	}

	public static long stringToSeconds(SimpleDateFormat sdf, String string) throws ParseException {
		return sdf.parse(string).getTime() / 1000;
	}

	public static String replaceBlank(String str) {
		if (null != str) {
			String dest = "";
			if (str != null) {
				Pattern p = Pattern.compile("\\s+|\\t+|\\r+|\\n+");
				Matcher m = p.matcher(str);
				dest = m.replaceAll("");
			}
			return dest;
		} else
			return "";
	}

	@SuppressWarnings("unchecked")
	public static <T> T coalesce(T... ts) {
		if (ts.length == 1)
			return ts[0];
		else if (ts.length == 2)
			return ts[0] != null ? ts[0] : ts[1];
		else
			return ts[0] != null ? ts[0] : coalesce(Arrays.copyOfRange(ts, 1, ts.length));
	}

	public static LinkedList<Pair<String, Integer>> getColumnIndex(String table, List<String> list) throws ColumnMapException {
		if (MetaDataLoader.COLUMNMAP.containsKey(table)) {
			LinkedList<Pair<String, Integer>> indexList = new LinkedList<Pair<String, Integer>>();
			HashMap<String, Integer> map = MetaDataLoader.COLUMNMAP.get(table);
			for (String string : list) {
				if (string.equals("*")) {
					for (String _k : map.keySet()) {
						indexList.add(new Pair<String, Integer>(_k, map.get(_k)));
					}
				} else if (map.containsKey(string))
					indexList.add(new Pair<String, Integer>(string, map.get(string)));
				else
					indexList.add(new Pair<String, Integer>(string, Integer.MAX_VALUE));
			}
			return indexList;
		} else {
			throw new ColumnMapException("COLUMNMAP does not contain [ " + table + " ]");
		}
	}

	public static String stringAppend(StringBuilder sb, Object... strings) {
		for (Object o : strings) {
			sb.append(o.toString());
		}
		return sb.toString();
	}

	public static void deleteFiles(Path path) throws IOException {
		Files.walkFileTree(path, EnumSet.allOf(FileVisitOption.class), Integer.MAX_VALUE, new FileVisitor<Path>() {

			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				if (null != exc)
					throw exc;
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}

		});
	}
}