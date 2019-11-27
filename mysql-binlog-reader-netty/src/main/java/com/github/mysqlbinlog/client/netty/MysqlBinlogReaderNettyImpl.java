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
import com.github.mysql.protocol.model.AuthenticateCmdPacket;
import com.github.mysql.protocol.model.ErrorResponsePacket;
import com.github.mysql.protocol.model.GreetingResponsePacket;
import com.github.mysql.protocol.model.RawMysqlPacket;
import com.github.mysqlbinlogreader.common.AbstractMysqlBinlogReaderImpl;
import com.github.mysqlbinlogreader.common.Connection;
import com.github.mysqlbinlogreader.common.exception.RuntimeMysqlBinlogClientException;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.ByteArrayInputStream;

public class MysqlBinlogReaderNettyImpl extends AbstractMysqlBinlogReaderImpl {
/*    private EventLoopGroup workerGroup;

    private String masterHostname;
    private int masterPort;

    private int readTimeout;
*/
    
    private String username;
    private String password;

    @Override
    protected Connection initializeConnection() {
        
        return this.getConnection();
        
/*        this.workerGroup = new NioEventLoopGroup();

        NettyConnectionImpl connection = new NettyConnectionImpl(workerGroup);
        connection.setReadTimeout(readTimeout);
        connection.setHostName(masterHostname);
        connection.setPort(masterPort);
        
        return connection;
*/    }
    
    @Override
    public void connect() {
        super.connect();
        authenticate();
    }
    

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.communication.netty.MysqlBinlogClient#authenticate()
     */
    public void authenticate() {

        RawMysqlPacket packet = getConnection().readRawPacket();
        GreetingResponsePacket greetingResponsePacket = 
                (GreetingResponsePacket) getGreetingResponsePacketDeserializer().deserialize(
                        new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));

        AuthenticateCmdPacket authenticateCmdPacket = 
                new AuthenticateCmdPacket(null, this.username, this.password, "utf-8", 
                        greetingResponsePacket.getScramble(), 0, greetingResponsePacket.getServerCollation());
        getConnection().writeRawPacket(authenticateCmdPacket);

        packet = getConnection().readRawPacket();

        if (packet.isErrorPacket()) {
            ErrorResponsePacket errorResponsePacket = 
                    (ErrorResponsePacket) getErrorResponsePacketDeserializer().deserialize(
                            new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
            onMysqlError(errorResponsePacket);
        }

        if (!packet.isOKPacket()) {
            throw new RuntimeMysqlBinlogClientException("Authentication ERROR [ Unknown ]");
        }
    }


/*
    @Override
    public void close() {
        super.close();
        if (this.workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

    public String getMasterHostname() {
        return masterHostname;
    }

    public void setMasterHostname(String masterHostname) {
        this.masterHostname = masterHostname;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }
*/
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
