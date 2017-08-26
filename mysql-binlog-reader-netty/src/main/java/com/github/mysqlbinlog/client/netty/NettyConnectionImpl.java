/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.github.mysqlbinlog.client.netty;

import com.github.mysql.protocol.model.RawMysqlPacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NettyConnectionImpl extends ChannelInboundHandlerAdapter implements Connection {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyConnectionImpl.class);
    private static int DEFAULT_READTIMEOUT = 300;

    private EventLoopGroup eventLoopGroup;
    private ChannelPipeline pipeline;
    private int readTimeout;
    private Throwable lastCause;

    private LinkedBlockingQueue<RawMysqlPacket> queuePackets = new LinkedBlockingQueue<RawMysqlPacket>();

    public NettyConnectionImpl() {
        this.setReadTimeout(DEFAULT_READTIMEOUT);
    }

    public NettyConnectionImpl(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
        this.setReadTimeout(DEFAULT_READTIMEOUT);
    }


    @Override
    public void connect(String hostname, int port) {
        Bootstrap bootstrap = new Bootstrap();

        bootstrap.group(this.eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new MysqlBinlogReaderChannelInitializer(this));

        bootstrap.option(ChannelOption.AUTO_READ, false);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.SO_RCVBUF, 65535);
        bootstrap.option(ChannelOption.AUTO_READ, false);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);

        ChannelFuture future = bootstrap.connect(hostname, port);

        try {
            future.sync();

            future.channel().config().setAutoRead(false);
            this.pipeline = future.channel().pipeline();
        } catch (Exception ex) {
            LOGGER.error("Connection Error [" + ex.getMessage() + "]", ex);
            throw new RuntimeException(ex);
        }

    }


    @Override
    public RawMysqlPacket readRawPacket() {
        if (queuePackets.size() > 0) {
            RawMysqlPacket packet = queuePackets.poll();
            
            if (packet.isEmpty()) {
                throw new RuntimeException((this.lastCause == null ? null : this.lastCause));
            }
            
            return packet;
        }

        pipeline.read();
        
        try {
            RawMysqlPacket packet = queuePackets.poll(this.getReadTimeout(), TimeUnit.SECONDS);
            
            if (packet == null) {
                throw new TimeoutException();
            }
            
            if (packet.isEmpty()) {
                throw new RuntimeException((this.lastCause == null ? null : this.lastCause));
            }
            
            return packet;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeRawPacket(Object msg) {
        pipeline.writeAndFlush(msg);
    }

    @Override
    public void close() {
        if (this.pipeline != null) {
            pipeline.close();
        }
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Channel Activated");
        }
        ctx.read();
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RawMysqlPacket packet = (RawMysqlPacket) msg;
        queuePackets.add(packet);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.error("Exception Caught [" + cause.getMessage() + "]", cause);
        this.lastCause = cause;
        queuePackets.add(new RawMysqlPacket() );
        ctx.close();
    }

    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }
    
    public void setEventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }

    public int getReadTimeout() {
        return readTimeout;
    }
    
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }
}
