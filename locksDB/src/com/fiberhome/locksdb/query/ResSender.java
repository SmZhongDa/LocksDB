package com.fiberhome.locksdb.query;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.client.LocksMsg;
import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.LocksUtil;

public class ResSender extends LocksQuery implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(ResSender.class);
	private final ChannelHandlerContext ctx;
	private final LinkedBlockingQueue<String> response;
	private final ResultHandler resultHandler;
	private final CountDownLatch latch;
	private final LinkedList<ChannelFuture> futureList = new LinkedList<ChannelFuture>();
	private int get = 0;
	private int put = 0;

	public ResSender(CountDownLatch latch, ChannelHandlerContext ctx, LinkedBlockingQueue<String> response, ResultHandler resultHandler, String rid) {
		super(rid);
		this.latch = latch;
		this.ctx = ctx;
		this.response = response;
		this.resultHandler = resultHandler;
	}

	private void send() {
		String msg;
		while ((msg = response.poll()) != null) {
			get++;
			StringBuilder sb = new StringBuilder();
			String string = LocksUtil.stringAppend(sb, System.currentTimeMillis(), "\t1\t", rid, "\t", msg);
			byte[] bytes = string.getBytes(Config.DEFAULTCHARSET);
			futureList.add(ctx.writeAndFlush(new LocksMsg(bytes.length, bytes)));
		}
//		ctx.flush();
	}

	@Override
	public void run() {
		try {
			while (!resultHandler.isDone()) {
				send();
			}
			if (!response.isEmpty())
				send();
			while (!futureList.isEmpty())
				iterate();
		} finally {
			latch.countDown();
			isDone = true;
			logger.info("ResSender {} get : {} , put : {}", rid, get, put);
		}
	}

	private void iterate() {
		Iterator<ChannelFuture> it = futureList.iterator();
		while (it.hasNext()) {
			if (it.next().isDone()) {
				it.remove();
				put++;
			}
		}
	}

}
