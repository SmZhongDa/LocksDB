package com.fiberhome.locksdb.server;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.queryparser.classic.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.client.LocksMsg;
import com.fiberhome.locksdb.client.request.LocksRequest;
import com.fiberhome.locksdb.query.CountQuery;
import com.fiberhome.locksdb.query.CountWithFilterQuery;
import com.fiberhome.locksdb.query.DimensionQuery;
import com.fiberhome.locksdb.query.LuceneAggQuery;
import com.fiberhome.locksdb.query.LuceneQuery;
import com.fiberhome.locksdb.query.Pair;
import com.fiberhome.locksdb.query.ResAggSender;
import com.fiberhome.locksdb.query.ResIdSender;
import com.fiberhome.locksdb.query.ResSender;
import com.fiberhome.locksdb.query.ResultAgg;
import com.fiberhome.locksdb.query.ResultAggHandler;
import com.fiberhome.locksdb.query.ResultHandler;
import com.fiberhome.locksdb.query.ResultIdHandler;
import com.fiberhome.locksdb.query.RocksAggQuery;
import com.fiberhome.locksdb.query.RocksIdQuery;
import com.fiberhome.locksdb.query.RocksQuery;
import com.fiberhome.locksdb.query.SubDimensionQuery;
import com.fiberhome.locksdb.util.ColumnMapException;
import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.ConfigLoader;
import com.fiberhome.locksdb.util.LocksUtil;

public class LocksServerHandler extends ChannelInboundHandlerAdapter {

	private Logger logger = LoggerFactory.getLogger(LocksServerHandler.class);

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		logger.error(cause.toString());
		ctx.close();
		logger.error("close connection : {}", ctx.channel().remoteAddress());
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) {
		logger.info("client connected : {}", ctx.channel().remoteAddress());
	}

	@Override
	public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof LocksRequest) {
			final long time = System.currentTimeMillis();
			final LocksRequest req = (LocksRequest) msg;
			new Thread(new Runnable() {

				@Override
				public void run() {
					
					String string = new String(req.msg, Config.DEFAULTCHARSET);
					String[] strs = string.split("\t", -1);
					String time = strs[0];
					long sendTime = Long.parseLong(time);
					String type = strs[1];
					if (type.equals("0"))
						query(sendTime, strs);
					if (type.equals("1"))
						count(sendTime, strs);
					if (type.equals("2"))
						countWithFilter(sendTime, strs);
					if (type.equals("3"))
						dimensionQuery(sendTime, strs);
					if (type.equals("4"))
						subDimensionQuery(sendTime, strs);
					if (type.equals("5"))
						queryId(sendTime, strs);
					if (type.equals("6"))
						queryAgg(sendTime, strs);
				}
				
				private void queryAgg(final long sendTime, String[] strs){
					final String rid = strs[2];
					final String query = strs[3];
					final String partitions = strs[4];
					final String table = strs[5];
					final int limit = Integer.parseInt(strs[6]);
					final int timeout = Integer.parseInt(strs[7]);
					final boolean orderby = Boolean.parseBoolean(strs[8]);
					final boolean desc = Boolean.parseBoolean(strs[9]);
					final boolean count = Boolean.parseBoolean(strs[10]);
					final String aggType = strs[11];
					final String aggColumn = strs[12];
					final boolean isAgg = Boolean.parseBoolean(strs[13]);
					final boolean isGroupby = Boolean.parseBoolean(strs[14]);
					final String groupbyColumn = strs[15];
					
					final List<String> columnList = new LinkedList<String>();
					for (int i = 16; i < strs.length; i++) {
						columnList.add(strs[i]);
					}
					String prtstr = (partitions.length() != 8) ? partitions.substring(0, 8)+"~"+partitions.substring(partitions.length()-8) : partitions;
					logger.info("receive LocksRequest {} from {} - [ {} , partitions : {} , table : {} , limit : {} , timeout : {} , columns : {} , order by : {} , desc : {} , count : {}" +
							", aggType : {}, aggColumn : {}, isAgg : {}, isGroupby : {} , groupbyColumn : {}] - elapsed time : {}", rid,
							ctx.channel().remoteAddress(), query, prtstr, table, limit, timeout, columnList, orderby, desc, count,aggType,aggColumn,isAgg,isGroupby,groupbyColumn,time - sendTime);
					ChannelFutureListener closer = new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) {
							// future.channel().close();
							logger.info("send LocksResponse {} to {} - elapsed time : {}", rid, ctx.channel().remoteAddress(), System.currentTimeMillis() - time);
						}
					};
					LuceneAggQuery luceneAggQuery = null;
					try {
						logger.debug("enter into queryAgg.........");
						ExecutorService service = Executors.newFixedThreadPool(5);
						CountDownLatch latch = new CountDownLatch(5);
						LinkedBlockingQueue<Pair<String, byte[]>> keyQueue = new LinkedBlockingQueue<Pair<String, byte[]>>(10000000);
						luceneAggQuery = new LuceneAggQuery(latch, query, orderby,keyQueue, partitions, table, rid);
						LinkedBlockingQueue<byte[]> result = new LinkedBlockingQueue<byte[]>(10000000);
						RocksAggQuery rocksAggQuery = new RocksAggQuery(latch, keyQueue, result, luceneAggQuery, rid);
						LinkedBlockingQueue<String> response = new LinkedBlockingQueue<String>(10000000);
						ResultAggHandler rh = new ResultAggHandler(latch, rocksAggQuery, result, LocksUtil.getColumnIndex(table, columnList), response, rid);
						LinkedBlockingQueue<String> responseAll = new LinkedBlockingQueue<String>(10000000);
						ResultAgg resultAgg = new ResultAgg(latch,rh,response,rid,aggType,aggColumn,isAgg,isGroupby,groupbyColumn,responseAll);
						ResAggSender aggSender = new ResAggSender(latch, ctx, responseAll, resultAgg, rid);
						service.execute(luceneAggQuery);
						service.execute(rocksAggQuery);
						service.execute(rh);
						service.execute(resultAgg);
						service.execute(aggSender);
						service.shutdown();
						latch.await(timeout, TimeUnit.SECONDS);
						luceneAggQuery.shutdown();
						finish(rid, closer);
						logger.debug("server {} query is done", rid);
					} catch (ParseException e) {
						logger.error("parse {} query failed : {}", rid, e.toString());
						errorFinish(rid, closer, e);
					} catch (InterruptedException | ColumnMapException e) {
						logger.error("rid " + rid + " - " + e.toString());
						errorFinish(rid, closer, e);
						luceneAggQuery.shutdown();
					}
				
					
				}
				
				
				

				private void queryId(final long sendTime, String[] strs) {
					String table;
					final String rid = strs[2];
					final String idTotal = strs[3];
					final int timeout = Integer.parseInt(strs[5]);
					final boolean orderby = Boolean.parseBoolean(strs[6]);
					final boolean desc = Boolean.parseBoolean(strs[7]);
					final boolean count = Boolean.parseBoolean(strs[8]);
					final int limit = Integer.parseInt(strs[9]);
					final List<String> idList = new LinkedList<String>();

					final List<String> columnList = new LinkedList<String>();
					for (int i = 10; i < strs.length; i++) {
						columnList.add(strs[i]);
					}

					String[] idGroup = idTotal.split(":");
					for (int i = 0; i < idGroup.length; i++) {
						idList.add(idGroup[i]);
					}

					if (idList.size() > 0) {
						table = ConfigLoader.IDTOPROTOCOLMAP.get(idList.get(0).substring(0, 3));
					} else {
						table = "auth";
					}

					logger.info("receive LocksRequest {} from {} - [ {} , timeout : {} , columns : {} , order by : {} , desc : {} , count : {} ] - elapsed time : {}", rid, ctx.channel().remoteAddress(), idList, timeout, columnList, orderby,
							desc, count, time - sendTime);
					ChannelFutureListener closer = new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) {
							// future.channel().close();
							logger.info("send LocksResponse {} to {} - elapsed time : {}", rid, ctx.channel().remoteAddress(), System.currentTimeMillis() - time);
						}
					};
					try {
						ExecutorService service = Executors.newFixedThreadPool(4);
						CountDownLatch latch = new CountDownLatch(3);
						LinkedBlockingQueue<byte[]> result = new LinkedBlockingQueue<byte[]>();
						RocksIdQuery rocksQuery = new RocksIdQuery(latch, result, rid, idList);
						LinkedBlockingQueue<String> response = new LinkedBlockingQueue<String>(limit);
						ResultIdHandler rh = new ResultIdHandler(latch, rocksQuery, result, LocksUtil.getColumnIndex(table, columnList), response, rid);
						ResIdSender sender = new ResIdSender(latch, ctx, response, rh, rid);
						service.execute(rocksQuery);
						service.execute(rh);
						service.execute(sender);
						service.shutdown();
						latch.await(timeout, TimeUnit.SECONDS);
						logger.debug("will close.");
						finish(rid, closer);
						logger.debug("server {} query is done", rid);
					} catch (InterruptedException | ColumnMapException e) {
						logger.error("rid " + rid + " - " + e.toString());
						errorFinish(rid, closer, e);
					}

				}

				private void countWithFilter(final long sendTime, String[] strs) {
					final String rid = strs[2];
					final String query = strs[3];
					final String partitions = strs[4];
					final String table = strs[5];
					final int timeout = Integer.parseInt(strs[6]);					
					
					String prtstr = (partitions.length() != 8) ? partitions.substring(0, 8)+"~"+partitions.substring(partitions.length()-8) : partitions;
					logger.info("receive CountWithFilterRequest {} from {} - [ {} , partitions : {} , table : {} , timeout : {} ] - elapsed time : {}", rid, ctx.channel().remoteAddress(), query, prtstr, table, timeout, time - sendTime);
					ChannelFutureListener closer = new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) {
							// future.channel().close();
							logger.info("send CountWithFilterResponse {} to {} - elapsed time : {}", rid, ctx.channel().remoteAddress(), System.currentTimeMillis() - time);
						}
					};
					CountWithFilterQuery countWithFilterQuery = null;
					try {
						ExecutorService service = Executors.newFixedThreadPool(1);
						CountDownLatch latch = new CountDownLatch(1);
						countWithFilterQuery = new CountWithFilterQuery(query, partitions, table, ctx, latch, rid);
						service.execute(countWithFilterQuery);
						service.shutdown();
						latch.await(timeout, TimeUnit.SECONDS);
						countWithFilterQuery.shutdown();
						finish(rid, closer);
						logger.debug("server {} countWithFilter is done", rid);
					} catch (ParseException e) {
						logger.error("parse {} query failed : {}", rid, e.toString());
						errorFinish(rid, closer, e);
					} catch (InterruptedException e) {
						logger.error("rid " + rid + " - " + e.toString());
						errorFinish(rid, closer, e);
						countWithFilterQuery.shutdown();
					}
				}

				private void count(final long sendTime, String[] strs) {
					final String rid = strs[2];
					final String partitions = strs[3];
					final int timeout = Integer.parseInt(strs[4]);
					final List<String> tableList = new LinkedList<String>();
					for (int i = 5; i < strs.length; i++) {
						tableList.add(strs[i]);
					}
					logger.info("receive CountRequest {} from {} - [ table : {} , timeout : {} ] - elapsed time : {}", rid, ctx.channel().remoteAddress(), tableList, timeout, time - sendTime);
					ChannelFutureListener closer = new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) {
							// future.channel().close();
							logger.info("send CountResponse {} to {} - elapsed time : {}", rid, ctx.channel().remoteAddress(), System.currentTimeMillis() - time);
						}
					};
					ExecutorService service = Executors.newFixedThreadPool(1);
					CountDownLatch latch = new CountDownLatch(1);
					CountQuery query = new CountQuery(tableList, partitions, ctx, latch, rid);
					try {
						service.execute(query);
						service.shutdown();
						latch.await(timeout, TimeUnit.SECONDS);
						query.shutdown();
						finish(rid, closer);
						logger.debug("server {} count is done", rid);
					} catch (InterruptedException e) {
						logger.error("rid " + rid + " - " + e.toString());
						errorFinish(rid, closer, e);
						query.shutdown();
					}
				}

				private void query(final long sendTime, String[] strs) {
					final String rid = strs[2];
					final String query = strs[3];
					final String partitions = strs[4];
					final String table = strs[5];
					final int limit = Integer.parseInt(strs[6]);
					final int timeout = Integer.parseInt(strs[7]);
					final boolean orderby = Boolean.parseBoolean(strs[8]);
					final boolean count = Boolean.parseBoolean(strs[10]);
					final List<String> columnList = new LinkedList<String>();
					for (int i = 16; i < strs.length; i++) {
						columnList.add(strs[i]);
					}
					String prtstr = (partitions.length() != 8) ? partitions.substring(0, 8)+"~"+partitions.substring(partitions.length()-8) : partitions;
					logger.info("receive LocksRequest {} from {} - [ {} , partitions : {} , table : {} , limit : {} , timeout : {} , columns : {} , order by : {} , desc : {} , count : {} ] - elapsed time : {}", rid,
							ctx.channel().remoteAddress(), query, prtstr, table, limit, timeout, columnList, orderby, count, time - sendTime);
					ChannelFutureListener closer = new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) {
							// future.channel().close();
							logger.info("send LocksResponse {} to {} - elapsed time : {}", rid, ctx.channel().remoteAddress(), System.currentTimeMillis() - time);
						}
					};
					LuceneQuery luceneQuery = null;
					ExecutorService service = Executors.newFixedThreadPool(4);
					try {
						
						CountDownLatch latch = new CountDownLatch(4);
						LinkedBlockingQueue<Pair<String, byte[]>> keyQueue = new LinkedBlockingQueue<Pair<String, byte[]>>(limit);
						luceneQuery = new LuceneQuery(latch, query, keyQueue, partitions, table, limit, orderby, count, rid, ctx);
						LinkedBlockingQueue<byte[]> result = new LinkedBlockingQueue<byte[]>(limit);
						RocksQuery rocksQuery = new RocksQuery(latch, keyQueue, result, luceneQuery, rid);
						LinkedBlockingQueue<String> response = new LinkedBlockingQueue<String>(limit);
						ResultHandler rh = new ResultHandler(latch, rocksQuery, result, LocksUtil.getColumnIndex(table, columnList), response, rid);
						ResSender sender = new ResSender(latch, ctx, response, rh, rid);
						service.execute(luceneQuery);
						service.execute(rocksQuery);
						service.execute(rh);
						service.execute(sender);
						service.shutdown();
						latch.await(timeout, TimeUnit.SECONDS);
						luceneQuery.shutdown();
						finish(rid, closer);
						logger.info("server {} query is done", rid);
					} catch (ParseException e) {
						logger.error("parse {} query failed : {}", rid, e.toString());
						errorFinish(rid, closer, e);
					} catch (InterruptedException | ColumnMapException e) {
						logger.error("rid " + rid + " - " + e.toString());
						errorFinish(rid, closer, e);
						luceneQuery.shutdown();
					}finally{
						service.shutdownNow();
					}
				}

				private void dimensionQuery(long sendTime, String[] strs) {
					final String rid = strs[2];
					final String query = strs[3];
					final String partitions = strs[4];
					final int timeout = Integer.parseInt(strs[5]);
					final int sampleSize = Integer.parseInt(strs[6]);
					final List<String> tableList = new LinkedList<String>();
					for (int i = 7; i < strs.length; i++) {
						tableList.add(strs[i]);
					}
					String prtstr = (partitions.length() != 8) ? partitions.substring(0, 8)+"~"+partitions.substring(partitions.length()-8) : partitions;
					logger.info("receive DimensionRequest {} from {} - [ {} , table : {} , partitions : {} , timeout : {} , sampleSize : {} ] - elapsed time : {}", rid, ctx.channel().remoteAddress(), query, tableList, prtstr, timeout,
							sampleSize, time - sendTime);
					ChannelFutureListener closer = new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) {
							// future.channel().close();
							logger.info("send DimensionResponse {} to {} - elapsed time : {}", rid, ctx.channel().remoteAddress(), System.currentTimeMillis() - time);
						}
					};
					CountDownLatch latch = new CountDownLatch(tableList.size());
					List<DimensionQuery> queryList = new LinkedList<DimensionQuery>();
					try {
						for (String table : tableList) {
							queryList.add(new DimensionQuery(query, table, partitions, ctx, latch, sampleSize, rid));
						}
					} catch (ParseException e) {
						logger.error("parse {} query failed : {}", rid, e.toString());
						errorFinish(rid, closer, e);
						return;
					}
					ExecutorService service = Executors.newFixedThreadPool(tableList.size());
					for (DimensionQuery q : queryList) {
						service.execute(q);
					}
					service.shutdown();
					try {
						latch.await(timeout, TimeUnit.SECONDS);
						finish(rid, closer);
						logger.debug("server {} dimensionQuery is done", rid);
					} catch (InterruptedException e) {
						logger.error("rid " + rid + " - " + e.toString());
						errorFinish(rid, closer, e);
						for (DimensionQuery q : queryList) {
							q.shutdown();
						}
					}
				}

				private void subDimensionQuery(long sendTime, String[] strs) {
					final String rid = strs[2];
					final String query = strs[3];
					final String partitions = strs[4];
					final int timeout = Integer.parseInt(strs[5]);
					final int sampleSize = Integer.parseInt(strs[6]);
					final String dim = strs[7];
					final List<String> path = new LinkedList<String>();
					int i = 8;
					for (; i < strs.length; i++) {
						if (!strs[i].equals(""))
							path.add(strs[i]);
						else
							break;
					}
					i++;
					final List<String> tableList = new LinkedList<String>();
					for (; i < strs.length; i++) {
						tableList.add(strs[i]);
					}
					String prtstr = (partitions.length() != 8) ? partitions.substring(0, 8)+"~"+partitions.substring(partitions.length()-8) : partitions;
					logger.info("receive SubDimensionRequest {} from {} - [ {} , table : {} , partitions : {} , timeout : {} , sampleSize : {} , dim : {} , path : {} ] - elapsed time : {}", rid, ctx.channel().remoteAddress(), query,
							tableList, prtstr, timeout, sampleSize, dim, path, time - sendTime);
					ChannelFutureListener closer = new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) {
							// future.channel().close();
							logger.info("send subDimensionResponse {} to {} - elapsed time : {}", rid, ctx.channel().remoteAddress(), System.currentTimeMillis() - time);
						}
					};
					CountDownLatch latch = new CountDownLatch(tableList.size());
					List<SubDimensionQuery> queryList = new LinkedList<SubDimensionQuery>();
					try {
						for (String s : tableList) {
							queryList.add(new SubDimensionQuery(query, s, partitions, ctx, latch, sampleSize, rid, dim, path.toArray(new String[0])));
						}
					} catch (ParseException e) {
						logger.error("parse {} query failed : {}", rid, e.toString());
						errorFinish(rid, closer, e);
						return;
					}
					ExecutorService service = Executors.newFixedThreadPool(tableList.size());
					for (SubDimensionQuery q : queryList) {
						service.execute(q);
					}
					service.shutdown();
					try {
						latch.await(timeout, TimeUnit.SECONDS);
						finish(rid, closer);
						logger.debug("server {} subDimensionQuery is done", rid);
					} catch (InterruptedException e) {
						logger.error("rid " + rid + " - " + e.toString());
						errorFinish(rid, closer, e);
						for (SubDimensionQuery q : queryList) {
							q.shutdown();
						}
					}
				}

				private void finish(final String rid, ChannelFutureListener closer) {
					StringBuilder sb = new StringBuilder();
					String _s = LocksUtil.stringAppend(sb, System.currentTimeMillis(), "\t0\t", rid);
					byte[] _bs = _s.getBytes(Config.DEFAULTCHARSET);
					ctx.writeAndFlush(new LocksMsg(_bs.length, _bs)).addListener(closer);
				}

				private void errorFinish(final String rid, ChannelFutureListener closer, Exception e) {
					StringBuilder sb = new StringBuilder();
					String _s = LocksUtil.stringAppend(sb, System.currentTimeMillis(), "\t2\t", rid, "\t", e);
					byte[] _bs = _s.getBytes(Config.DEFAULTCHARSET);
					ctx.writeAndFlush(new LocksMsg(_bs.length, _bs)).addListener(closer);
				}

			}).start();
		} else {
			logger.info("invalid request from {}", ctx.channel().remoteAddress());
		}
		ReferenceCountUtil.release(msg);
	}
}
