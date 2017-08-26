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

import com.github.mysql.protocol.model.AuthenticateCmdPacket;
import com.github.mysql.protocol.model.CmdPacket;
import com.github.mysql.protocol.model.RawMysqlPacket;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;

public class MysqlRawPacketEncoder extends MessageToByteEncoder<CmdPacket> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlRawPacketEncoder.class);
    
    public MysqlRawPacketEncoder() {
        super();
    }

    /* (non-Javadoc)
     * @see io.netty.handler.codec.MessageToByteEncoder#encode(io.netty.channel.ChannelHandlerContext, java.lang.Object, io.netty.buffer.ByteBuf)
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, CmdPacket msg, ByteBuf out) throws Exception {
        RawMysqlPacket packet = new RawMysqlPacket();
        packet.setRawBody(msg.getBody());
        packet.setLength(packet.getRawBody().length);
        packet.setSequence(msg instanceof AuthenticateCmdPacket ? 1 : 0);
        
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("SENDING [" + packet + "][" + DatatypeConverter.printHexBinary(packet.getFullBody()) + "]");
        }
        
        
        out.writeBytes(packet.getFullBody());
    }

}
