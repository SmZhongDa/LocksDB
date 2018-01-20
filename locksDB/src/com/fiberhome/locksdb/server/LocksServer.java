package com.fiberhome.locksdb.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocksServer implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(LocksServer.class);
	private final int port;

	public LocksServer(int port) {
		this.port = port;
	}

	public void open() throws InterruptedException {
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					ch.pipeline().addLast(new LocksServerDecoder(1 << 20, 4), new LocksServerEncoder(), new LocksServerHandler());
				}
			});
			ChannelFuture f = b.bind(port).sync();
			logger.info("LocksServer running ......");
			f.channel().closeFuture().sync();
		} finally {
			workerGroup.shutdownGracefully().sync();
			bossGroup.shutdownGracefully().sync();
		}
	}

	@Override
	public void run() {
		try {
			open();
		} catch (InterruptedException e) {
			logger.error("open netty server failed : {}", e.toString());
			System.exit(1);
		}
	}

}
