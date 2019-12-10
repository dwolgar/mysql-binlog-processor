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


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mysql.io.MysqlBinlogByteArrayInputStream;
import com.github.mysql.protocol.deserializer.RawResponsePacketDeserializer;
import com.github.mysql.protocol.deserializer.ResponsePacketDeserializer;
import com.github.mysql.protocol.model.AuthenticateCmdPacket;
import com.github.mysql.protocol.model.CmdPacket;
import com.github.mysql.protocol.model.RawMysqlPacket;
import com.github.mysqlbinlogreader.common.Connection;
import com.github.mysqlbinlogreader.common.exception.RuntimeMysqlBinlogClientException;

public class ConnectionJdbcImpl implements Connection {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionJdbcImpl.class);

    private String jdbcDriverClassName = "com.mysql.cj.jdbc.Driver";

    private ResponsePacketDeserializer responsePacketDeserializer;

    private MysqlStreamProvider mysqlStreamProvider;

    private java.sql.Connection connection;
    private InputStream inputStream;
    private OutputStream outputStream;

    private final String connectionString;
    private final String username;
    private final String password;

    public ConnectionJdbcImpl(java.sql.Connection connection) {
        this.connection = connection;
        this.connectionString = null;
        this.username = null;
        this.password = null;
        this.responsePacketDeserializer = new RawResponsePacketDeserializer();
    }

    public ConnectionJdbcImpl(String connectionString, String username, String password) {
        this.connectionString = connectionString;
        this.username = username;
        this.password = password;
        this.responsePacketDeserializer = new RawResponsePacketDeserializer();
    }

    private void registerJdbcDriver() {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Loading db drivers...[{}]", jdbcDriverClassName);
            }

            boolean registered = false;
            Enumeration<Driver> drivers = DriverManager.getDrivers();
            if (drivers != null) {
                while (drivers.hasMoreElements()) {
                    Driver driver = drivers.nextElement();
                    if (logger.isDebugEnabled()) {
                        logger.debug("Registered Driver [{}]", driver.getClass().getCanonicalName());
                    }
                    if (this.jdbcDriverClassName.equalsIgnoreCase(driver.getClass().getCanonicalName())) {
                        registered = true;
                        break;
                    }
                }
            }

            if (!registered) {
                DriverManager.registerDriver((Driver) Class.forName(jdbcDriverClassName).newInstance());
            }
        } catch (Exception e) {
            logger.error("Didn't find any Mysql Driver", e);
        }

    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.client.jdbc.Connection#connect()
     */
    public void connect() {
        if (this.connection == null) {
            try {
                registerJdbcDriver();

                this.connection = DriverManager.getConnection(this.connectionString, this.username, this.password);
            } catch (Exception e) {
                throw new RuntimeMysqlBinlogClientException(e);
            }
        } 

        if (this.mysqlStreamProvider == null) {
            this.mysqlStreamProvider = new ConnectionBasedMysqlStreamProviderImpl(this.connection);
        }
        mysqlStreamProvider.retrieveStreams();
        this.inputStream = mysqlStreamProvider.getInputStream();
        this.outputStream = mysqlStreamProvider.getOutputStream();
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.client.jdbc.Connection#close()
     */
    public void close() {
        try {
            this.connection.close();
        } catch (Exception e) {
            throw new RuntimeMysqlBinlogClientException(e);
        }
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.client.jdbc.Connection#readRawPacket()
     */
    public RawMysqlPacket readRawPacket() {
        MysqlBinlogByteArrayInputStream is = new MysqlBinlogByteArrayInputStream(this.inputStream);

        return (RawMysqlPacket) responsePacketDeserializer.deserialize(is);
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.client.jdbc.Connection#writeRawPacket(java.lang.Object)
     */
    public void writeRawPacket(CmdPacket msg) {

        try {
            RawMysqlPacket packet = new RawMysqlPacket();
            packet.setRawBody(msg.getBody());
            packet.setLength(packet.getRawBody().length);
            packet.setSequence(msg instanceof AuthenticateCmdPacket ? 1 : 0);

            this.outputStream.write(packet.getFullBody());
            this.outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeMysqlBinlogClientException(e);
        }
    }

    public java.sql.Connection getConnection() {
        return connection;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getJdbcDriverClassName() {
        return jdbcDriverClassName;
    }

    public void setJdbcDriverClassName(String jdbcDriverClassName) {
        this.jdbcDriverClassName = jdbcDriverClassName;
    }

    public MysqlStreamProvider getMysqlStreamProvider() {
        return mysqlStreamProvider;
    }

    public void setMysqlStreamProvider(MysqlStreamProvider mysqlStreamProvider) {
        this.mysqlStreamProvider = mysqlStreamProvider;
    }

}
