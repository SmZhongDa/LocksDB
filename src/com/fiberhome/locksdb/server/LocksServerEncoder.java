package com.fiberhome.locksdb.server;

import com.fiberhome.locksdb.client.LocksMsg;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class LocksServerEncoder extends MessageToByteEncoder<LocksMsg> {

	@Override
	protected void encode(ChannelHandlerContext ctx, LocksMsg msg, ByteBuf out) throws Exception {
		if (null != msg) {
			out.writeInt(msg.length);
			out.writeBytes(msg.msg);
		}
	}

}
