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

package com.github.mysqlbinlog.client.jdbc;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.DriverManager;

import javax.xml.bind.DatatypeConverter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mysql.io.MysqlBinlogByteArrayInputStream;
import com.github.mysql.protocol.deserializer.RawResponsePacketDeserializer;
import com.github.mysql.protocol.model.QueryCmdPacket;
import com.github.mysql.protocol.model.RawMysqlPacket;
import com.github.mysqlbinlogreader.common.exception.RuntimeMysqlBinlogClientException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value= {ConnectionJdbcImpl.class})
//@RunWith(JUnit4.class)
public class ConnectionJdbcImplTest {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionJdbcImplTest.class);
    
    public class PipeMysqlStreamProviderImpl implements MysqlStreamProvider {
        
        final PipedOutputStream connectionOutput = new PipedOutputStream();
        final PipedInputStream  providerInput    = new PipedInputStream(connectionOutput);

        final PipedOutputStream providerOutput   = new PipedOutputStream();
        final PipedInputStream  connectionInput  = new PipedInputStream(providerOutput);

        public PipeMysqlStreamProviderImpl() throws Exception {
            
        }
        
        @Override
        public void retrieveStreams() {
            return;
        }
        
        @Override
        public OutputStream getOutputStream() {
            return this.connectionOutput;
        }
        
        @Override
        public InputStream getInputStream() {
            return connectionInput;
        }

        public PipedInputStream getProviderInput() {
            return providerInput;
        }

        public PipedOutputStream getProviderOutput() {
            return providerOutput;
        }
    }

    private String mysqlVersion;

    private static final String mysqlVersionProperty = "mysql.version";
    private static final String mysql5DriverName = "com.mysql.jdbc.Driver";
    private static final String mysql8DriverName = "com.mysql.cj.jdbc.Driver";
    private static final String connectionString = "jdbc:mysql://localhost:3310";
    private static final String username = "root";
    private static final String password = "";

    private ConnectionJdbcImpl connection;

    @Before
    public void init() {
        mysqlVersion = System.getProperty(mysqlVersionProperty);
        if (mysqlVersion == null) {
            mysqlVersion = "mysql5";
        }
        logger.debug("Testing mysql version [{}]", mysqlVersion);
        
    }
    
    @After
    public void finish() {

    }
    
    @Test(expected = RuntimeMysqlBinlogClientException.class)
    public void connectTest() throws Exception {
        PowerMockito.mockStatic(DriverManager.class);
        
        connection = new ConnectionJdbcImpl(connectionString, username, password);
        connection.setJdbcDriverClassName((mysqlVersion.equalsIgnoreCase("mysql5") ? mysql5DriverName: mysql8DriverName));
        connection.connect();
        assertNull(connection.getInputStream());
        assertNull(connection.getOutputStream());
        
    }
    
    @Test
    public void connectionTest() throws Exception {
        PowerMockito.mockStatic(DriverManager.class);

        MysqlStreamProvider mysqlStreamProvider = new PipeMysqlStreamProviderImpl(); 
        connection = new ConnectionJdbcImpl(connectionString, username, password);
        connection.setJdbcDriverClassName((mysqlVersion.equalsIgnoreCase("mysql5") ? mysql5DriverName: mysql8DriverName));
        connection.setMysqlStreamProvider(mysqlStreamProvider);
        
        
        connection.connect();
        assertNotNull(connection.getInputStream());
        assertNotNull(connection.getOutputStream());
    }
    
    @Test
    public void readRawPacketTest() throws Exception {
        PowerMockito.mockStatic(DriverManager.class);

        PipeMysqlStreamProviderImpl mysqlStreamProvider = new PipeMysqlStreamProviderImpl(); 
        connection = new ConnectionJdbcImpl(connectionString, username, password);
        connection.setJdbcDriverClassName((mysqlVersion.equalsIgnoreCase("mysql5") ? mysql5DriverName: mysql8DriverName));
        connection.setMysqlStreamProvider(mysqlStreamProvider);
        
        
        connection.connect();
        assertNotNull(connection.getInputStream());
        assertNotNull(connection.getOutputStream());

        OutputStream providerOutputStream = mysqlStreamProvider.getProviderOutput();
        RawMysqlPacket[] packets = new RawMysqlPacket[hex.length];
        for (int i = 0; i < hex.length; i++) {
            packets[i] = new RawMysqlPacket(hex[i].length()/2, ((i + 1) & 0xFF), DatatypeConverter.parseHexBinary(hex[i]));
            providerOutputStream.write(packets[i].getFullBody());
        }   
        providerOutputStream.flush();
        
        for (int i = 0; i < hex.length; i++) {
            RawMysqlPacket packet = connection.readRawPacket();
            assertArrayEquals(packets[i].getFullBody(), packet.getFullBody());
        }
        
    }

    @Test
    public void writeRawPacketTest() throws Exception {
        PowerMockito.mockStatic(DriverManager.class);

        PipeMysqlStreamProviderImpl mysqlStreamProvider = new PipeMysqlStreamProviderImpl(); 
        connection = new ConnectionJdbcImpl(connectionString, username, password);
        connection.setJdbcDriverClassName((mysqlVersion.equalsIgnoreCase("mysql5") ? mysql5DriverName: mysql8DriverName));
        connection.setMysqlStreamProvider(mysqlStreamProvider);
        
        
        connection.connect();
        assertNotNull(connection.getInputStream());
        assertNotNull(connection.getOutputStream());

        OutputStream providerOutputStream = mysqlStreamProvider.getProviderOutput();
        RawMysqlPacket[] packets = new RawMysqlPacket[hex.length];
        for (int i = 0; i < hex.length; i++) {
            packets[i] = new RawMysqlPacket(hex[i].length()/2, ((i + 1) & 0xFF), DatatypeConverter.parseHexBinary(hex[i]));
            providerOutputStream.write(packets[i].getFullBody());
        }   
        providerOutputStream.flush();
        
        for (int i = 0; i < hex.length; i++) {
            RawMysqlPacket packet = connection.readRawPacket();
            assertArrayEquals(packets[i].getFullBody(), packet.getFullBody());
        }
        
        InputStream providerInputStream = mysqlStreamProvider.getProviderInput();
        QueryCmdPacket cmd = new QueryCmdPacket("SHOW MASTER STATUS");
        connection.writeRawPacket(cmd);
        
        RawResponsePacketDeserializer responsePacketDeserializer = new RawResponsePacketDeserializer();
        MysqlBinlogByteArrayInputStream is = new MysqlBinlogByteArrayInputStream(providerInputStream);
        RawMysqlPacket packet = (RawMysqlPacket) responsePacketDeserializer.deserialize(is);

        assertArrayEquals(cmd.getBody(), packet.getRawBody());
    }

    @Test(expected = RuntimeMysqlBinlogClientException.class)
    public void closeTest() throws Exception {
        PowerMockito.mockStatic(DriverManager.class);
        
        PipeMysqlStreamProviderImpl mysqlStreamProvider = new PipeMysqlStreamProviderImpl(); 
        connection = new ConnectionJdbcImpl(connectionString, username, password);
        connection.setJdbcDriverClassName((mysqlVersion.equalsIgnoreCase("mysql5") ? mysql5DriverName: mysql8DriverName));
        connection.setMysqlStreamProvider(mysqlStreamProvider);
        
        
        connection.connect();
        assertNotNull(connection.getInputStream());
        assertNotNull(connection.getOutputStream());
        
        connection.close();
    }
    
    
    private static final String[] hex = new String[] {
            //receiveSettings
            "02",
            
            "036465660010676C6F62616C5F7661726961626C657310676C6F62616C5F7661726961626C65730D5661726961626C655F6E616D650D5661726961626C655F6E616D650CFF0000010000FD0110000000",
            "036465660010676C6F62616C5F7661726961626C657310676C6F62616C5F7661726961626C65730556616C75650556616C75650CFF0000100000FD0000000000",
            
            "1B61637469766174655F616C6C5F726F6C65735F6F6E5F6C6F67696E034F4646",
            
            "0D61646D696E5F6164647265737300",
            "0A61646D696E5F706F7274053333303632",
    };

}
