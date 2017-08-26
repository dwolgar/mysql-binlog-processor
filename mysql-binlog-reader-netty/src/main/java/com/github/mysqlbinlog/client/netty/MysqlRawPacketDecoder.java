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

import com.github.mysql.io.MysqlBinlogByteArrayInputStream;
import com.github.mysql.protocol.deserializer.ResponsePacketDeserializer;
import com.github.mysql.protocol.model.ResponsePacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MysqlRawPacketDecoder extends ReplayingDecoder<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlRawPacketDecoder.class);

    private ResponsePacketDeserializer responsePacketDeserializer;

    /* (non-Javadoc)
     * @see io.netty.handler.codec.ByteToMessageDecoder#decode(io.netty.channel.ChannelHandlerContext, io.netty.buffer.ByteBuf, java.util.List)
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        MysqlBinlogByteArrayInputStream is = new MysqlBinlogByteArrayInputStream(new ByteBufInputStream(in));

        ResponsePacket packet = responsePacketDeserializer.deserialize(is);
        out.add(packet);
        
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Decoded packet [" + packet + "]");
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
    }

    public ResponsePacketDeserializer getResponsePacketUnmarshaller() {
        return responsePacketDeserializer;
    }
    
    public void setResponsePacketUnmarshaller(ResponsePacketDeserializer responsePacketUnmarshaller) {
        this.responsePacketDeserializer = responsePacketUnmarshaller;
    }
}
