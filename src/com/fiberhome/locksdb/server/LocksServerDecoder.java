package com.fiberhome.locksdb.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import com.fiberhome.locksdb.client.request.LocksRequest.QueryBuilder;

public class LocksServerDecoder extends LengthFieldBasedFrameDecoder {

	private final int lengthFieldLength;

	public LocksServerDecoder(int maxFrameLength, int lengthFieldLength) throws Exception {
		super(maxFrameLength, 0, lengthFieldLength, 0, lengthFieldLength);
		this.lengthFieldLength = lengthFieldLength;
	}

	@Override
	public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		if (null == in)
			return null;
		if (in.readableBytes() < lengthFieldLength)
			return null;
		in.markReaderIndex();
		int length = in.readInt();
		if (in.readableBytes() < length) {
			in.resetReaderIndex();
			return null;
		}
		ByteBuf buf = in.readBytes(length);
		byte[] bytes = new byte[buf.readableBytes()];
		buf.readBytes(bytes);
		in.discardReadBytes();
		return QueryBuilder.getReq(length, bytes);
	}

}
