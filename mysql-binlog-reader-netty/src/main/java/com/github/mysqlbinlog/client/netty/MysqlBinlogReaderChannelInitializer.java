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


import com.github.mysql.protocol.deserializer.RawResponsePacketDeserializer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

public class MysqlBinlogReaderChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final ChannelInboundHandler channelInboundHandler;
    
    public MysqlBinlogReaderChannelInitializer(ChannelInboundHandler channelInboundHandler) {
        this.channelInboundHandler = channelInboundHandler;
    }

    public void beforeAdd(ChannelHandlerContext ctx) {
    }
    
    
    /* (non-Javadoc)
     * @see io.netty.channel.ChannelInitializer#initChannel(io.netty.channel.Channel)
     */
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline channelPipeline = ch.pipeline();
        
        MysqlRawPacketDecoder mysqlRawPacketDecoder = new MysqlRawPacketDecoder();
        RawResponsePacketDeserializer rawResponsePacketUnmarshaller = new RawResponsePacketDeserializer();
        mysqlRawPacketDecoder.setResponsePacketUnmarshaller(rawResponsePacketUnmarshaller);
        
        MysqlRawPacketEncoder mysqlRawpacketEncoder = new MysqlRawPacketEncoder();
        
        
        channelPipeline.addLast("mysqlRawpacketEncoder", mysqlRawpacketEncoder);
        channelPipeline.addLast("mysqlRawPacketDecoder", mysqlRawPacketDecoder);
        channelPipeline.addLast("mysqlRawPacketHandler", this.channelInboundHandler);
        
    }

}
