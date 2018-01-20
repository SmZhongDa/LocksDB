package com.fiberhome.locksdb.query;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultAgg extends LocksQuery implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(ResultAgg.class);
	private final LinkedBlockingQueue<String> response;
	private HashMap<String, Double> maxMinMap = new HashMap<String, Double>();
	private HashMap<String, Integer> countMap = new HashMap<String, Integer>();
	private final CountDownLatch latch;
	private ResultAggHandler rh;
	final String aggType;
	final String aggColumn;
	final boolean isAgg;
	final boolean isGroupby;
	final String groupbyColumn;
	private final LinkedBlockingQueue<String> responseAll;
	private String selectType;
	private Double globalMax = (double) -9223372036854775808L;
	private Double globalMin = (double) 9223372036854775807L;
	private List<String> distinctList = new ArrayList<String>();

	public ResultAgg(CountDownLatch latch, ResultAggHandler rh,
			LinkedBlockingQueue<String> response, String rid, String aggType,
			String aggColumn, boolean isAgg, boolean isGroupby,
			String groupbyColumn, LinkedBlockingQueue<String> responseAll) {
		super(rid);
		this.latch = latch;
		this.response = response;
		this.rh = rh;
		this.aggType = aggType;
		this.aggColumn = aggColumn;
		this.isAgg = isAgg;
		this.isGroupby = isGroupby;
		this.groupbyColumn = groupbyColumn;
		this.responseAll = responseAll;
	}

	public void init() throws InterruptedException {
		logger.debug("enter into init...");
		if (!isAgg && isGroupby)
			selectType = "001";
		if (!isGroupby) {
			if (aggType.equals("MIN"))
				selectType = "010";
			if (aggType.equals("MAX"))
				selectType = "011";
			if (aggType.equals("COUNT"))
				selectType = "100";
		}

		if (isAgg && isGroupby) {
			if (aggType.equals("MIN"))
				selectType = "101";
			if (aggType.equals("MAX"))
				selectType = "110";
			if (aggType.equals("COUNT"))
				selectType = "111";

		}

		logger.info("[ selectType : {}  , aggType : {} ,isAgg : {} ,aggColumn : {} ,isGroupby : {}, groupbyColumn : {}]",selectType, aggType, isAgg, aggColumn, isGroupby, groupbyColumn);
	}

	public void deal(boolean flag) throws InterruptedException {
		switch (this.selectType) {
		case "001":
			distinct("001", flag);
			break;
		case "010":
			onlyMin(flag);
			break;
		case "011":
			onlyMax(flag);
			break;
		case "100":
			onlyCount(flag);
			break;
		case "101":
			maxMin("101", flag);
			break;
		case "110":
			maxMin("110", flag);
			break;
		case "111":
			Count("111", flag);
			break;
		default:
			break;
		}

	}

	public void distinct(String type, boolean flag) throws InterruptedException {
		String line;
		StringBuilder sb;
		String[] strs = null;
		while (null != (line = this.response.poll())) {
			strs = line.split("\t");
			logger.debug(strs.length + " ");
			logger.debug(line);
			
			if (strs.length != 2) 
				continue;

			if (distinctList.contains(strs[1]))
				continue;
			distinctList.add(strs[1]);
		}
		if (flag) {
			for (String value : distinctList) {
				sb = new StringBuilder();
				sb.append(value).append("\t").append("true");
				this.responseAll.put(sb.toString());
			}
		}
	}

	public void Count(String type, boolean flag) throws InterruptedException {
		String line;
		StringBuilder sb;
		String[] strs = null;
		int indexGroup = 0;
		while (null != (line = this.response.poll())) {
			strs = line.split("\t");
			if (strs.length != 4) {
				logger.error("count(*)...group by has more than two column.query is correpted.");
				break;
			}
			if (strs[0].equals(groupbyColumn)) {
				indexGroup = 0;
			} else {
				indexGroup = 2;
			}

			if (countMap.containsKey(strs[indexGroup + 1]))
				countMap.put(strs[indexGroup + 1],countMap.get(strs[indexGroup + 1]) + 1);
			else
				countMap.put(strs[indexGroup + 1], 1);
		}
		if (flag) {
			sb = new StringBuilder();
			for (final Map.Entry<String, Integer> entry : countMap.entrySet()) {
				sb = new StringBuilder();
				sb.append(entry.getKey()).append("\t").append(String.valueOf(entry.getValue()));
				this.responseAll.put(sb.toString());
			}
		}

	}

	public void onlyMin(boolean flag) throws InterruptedException {
		String line;
		StringBuilder sb;
		boolean start = true;
		String[] strs;
		while (null != (line = this.response.poll())) {
			strs = line.split("\t");
			if (strs.length != 2)
				continue;
			if (strs[1].isEmpty() || null == strs[1])
				continue;
			double tmp = Double.parseDouble(strs[1]);
			if (start) {
				start = false;
				globalMin = tmp;
			}
			if (globalMin < tmp)
				continue;
			globalMin = tmp;
		}

		if (flag) {
			sb = new StringBuilder();
			String value = BigDecimal.valueOf(this.globalMin).toString();
			if (value.contains(".")) {
				String[] str = value.split("\\.");
				if (str[1].equals("0"))
					value = str[0];
			}
			sb.append(aggColumn).append("\t").append(value);
			this.responseAll.put(sb.toString());
		}

	}

	public void onlyMax(boolean flag) throws InterruptedException {
		String line;
		boolean start = true;
		StringBuilder sb;
		String[] strs;
		while (null != (line = this.response.poll())) {
			strs = line.split("\t");
			if (strs.length != 2)
				continue;
			if (strs[1].isEmpty() || null == strs[1])
				continue;
			double tmp = Double.parseDouble(strs[1]);
			if (start) {
				start = false;
				globalMax = tmp;
			}

			if (globalMax > tmp)
				continue;
			globalMax = tmp;
		}

		if (flag) {
			sb = new StringBuilder();
			String value = BigDecimal.valueOf(this.globalMax).toString();
			if (value.contains(".")) {
				String[] str = value.split("\\.");
				if (str[1].equals("0"))
					value = str[0];
			}

			sb.append(aggColumn).append("\t").append(value);
			this.responseAll.put(sb.toString());
		}

	}

	public void onlyCount(boolean flag) throws InterruptedException {
		StringBuilder sb = new StringBuilder();
		sb.append(aggColumn).append("\t").append(this.response.size());
		this.responseAll.put(sb.toString());
	}

	public void maxMin(String type, boolean flag) throws InterruptedException {
		String line;
		int indexGroup = 0;
		int indexAgg = 0;
		boolean start = false;
		StringBuilder sb;
		String[] strs = null;
		while (null != (line = this.response.poll())) {
			strs = line.split("\t");
			if (strs.length != 4)
				continue;
			if (start == false) {
				start = true;
				if (strs[0].equals(groupbyColumn)) {
					indexGroup = 0;
					indexAgg = 2;
				} else {
					indexGroup = 2;
					indexAgg = 0;
				}
			}

			if (strs[indexGroup + 1].isEmpty() || strs[indexAgg + 1].isEmpty())
				continue;

			if (type.equals("101")) {
				if (maxMinMap.containsKey(strs[indexGroup + 1])) {
					if (maxMinMap.get(strs[indexGroup + 1]) > Double.parseDouble(strs[indexAgg + 1]))
						maxMinMap.put(strs[indexGroup + 1],Double.parseDouble(strs[indexAgg + 1]));
					continue;
				}
				maxMinMap.put(strs[indexGroup + 1],Double.parseDouble(strs[indexAgg + 1]));
			} else {
				if (maxMinMap.containsKey(strs[indexGroup + 1])) {
					if (maxMinMap.get(strs[indexGroup + 1]) < Double.parseDouble(strs[indexAgg + 1]))
						maxMinMap.put(strs[indexGroup + 1],Double.parseDouble(strs[indexAgg + 1]));
					continue;
				}
				maxMinMap.put(strs[indexGroup + 1],Double.parseDouble(strs[indexAgg + 1]));
			}

		}

		if (flag) {
			for (final Map.Entry<String, Double> entry : maxMinMap.entrySet()) {
				sb = new StringBuilder();
				String value = BigDecimal.valueOf(entry.getValue()).toString();
				if (value.contains(".")) {
					String[] str = value.split("\\.");
					if (str[1].equals("0"))
						value = str[1];
				}
				sb.append(entry.getKey()).append("\t").append(value);
				responseAll.put(sb.toString());
			}
		}

	}

	@Override
	public void run() {
		logger.debug("enter into ResultAgg.........");
		try {
			init();
			while (!rh.isDone()) {
				Thread.sleep(1000);
				deal(false);
			}

			deal(true);

		} catch (Exception e) {
			logger.error("Agg happen error." + e.toString());
		} finally {
			latch.countDown();
			isDone = true;
		}
	}
}
