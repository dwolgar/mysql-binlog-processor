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


import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;

import com.github.mysqlbinlogreader.common.exception.RuntimeMysqlBinlogClientException;

public class ConnectionBasedMysqlStreamProviderImpl
        implements MysqlStreamProvider {
    
    private static final String mysql8ConnectionClassName = "com.mysql.cj.jdbc.ConnectionImpl";
    private static final String mysql5ConnectionClassName = "com.mysql.jdbc.ConnectionImpl";
    private static final String mysql5aConnectionClassName = "com.mysql.jdbc.JDBC4Connection";
    
    private final Object connection;
    private InputStream inputStream;
    private OutputStream outputStream;
    
    public ConnectionBasedMysqlStreamProviderImpl(Object connection) {
        this.connection = connection;
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.client.jdbc.MysqlStreamProvider#retrieveStreams()
     */
    @Override
    public void retrieveStreams() {
        if (this.connection == null) {
            throw new RuntimeMysqlBinlogClientException("Connection is null");
        }
        
        if (this.connection.getClass().getCanonicalName().equalsIgnoreCase(mysql8ConnectionClassName)) {
            retrieveMysql8Streams();
        } else if (this.connection.getClass().getCanonicalName().equalsIgnoreCase(mysql5ConnectionClassName)) {
            retrieveMysql5Streams();
        } else if (this.connection.getClass().getCanonicalName().equalsIgnoreCase(mysql5aConnectionClassName)) {
            retrieveMysql5aStreams();
        } else {
            throw new RuntimeMysqlBinlogClientException("Connection class [" + this.connection.getClass().getCanonicalName() + "] is not supported");
        }
        
    }
    
    private void retrieveMysql5aStreams() {
        retrieveMysql5Streams();
    }
    
    private void retrieveMysql5Streams() {
        try {
            Class<?> mysqlConnection5Class = Class.forName(mysql5ConnectionClassName);
            Field mysqlIoField = mysqlConnection5Class.getDeclaredField("io");
            mysqlIoField.setAccessible(true);
            Object mysqlIoObject = mysqlIoField.get(mysqlConnection5Class.cast(this.connection));
            
            Field mysqlInputField = mysqlIoObject.getClass().getDeclaredField("mysqlInput");
            mysqlInputField.setAccessible(true);
            Field mysqlOutput = mysqlIoObject.getClass().getDeclaredField("mysqlOutput");
            mysqlOutput.setAccessible(true);
            
            this.inputStream = (InputStream) mysqlInputField.get(mysqlIoObject);
            this.outputStream = (OutputStream) mysqlOutput.get(mysqlIoObject);
            
        } catch (Exception e) {
            throw new RuntimeMysqlBinlogClientException(e);
        }
    }
    
    private void retrieveMysql8Streams() {
        try {
            Class<?> mysqlConnection8Class = Class.forName(mysql8ConnectionClassName);
            MethodHandle getSessionMh = MethodHandles.lookup().findVirtual(mysqlConnection8Class, "getSession", MethodType.methodType(Class.forName("com.mysql.cj.NativeSession")));
            Object nativeSession = getSessionMh.invoke(this.connection);
            
            MethodHandle getProtocolMh = MethodHandles.lookup().findVirtual(nativeSession.getClass(), "getProtocol", MethodType.methodType(Class.forName("com.mysql.cj.protocol.a.NativeProtocol")));
            Object protocol = getProtocolMh.invoke(nativeSession);
            
            MethodHandle getSocketConnectionMh = MethodHandles.lookup().findVirtual(protocol.getClass(), "getSocketConnection", MethodType.methodType(Class.forName("com.mysql.cj.protocol.SocketConnection")));
            Object socketConnection = getSocketConnectionMh.invoke(protocol);
            
            MethodHandle getMysqlInputMh = MethodHandles.lookup().findVirtual(socketConnection.getClass(), "getMysqlInput", MethodType.methodType(Class.forName("com.mysql.cj.protocol.FullReadInputStream")));
            MethodHandle getMysqlOutputMh = MethodHandles.lookup().findVirtual(socketConnection.getClass(), "getMysqlOutput", MethodType.methodType(Class.forName("java.io.BufferedOutputStream")));
            
            this.inputStream = (InputStream) getMysqlInputMh.invoke(socketConnection);
            this.outputStream = (OutputStream) getMysqlOutputMh.invoke(socketConnection);

        } catch (Throwable e) {
            throw new RuntimeMysqlBinlogClientException(e);
        }
        
    }
    
    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.client.jdbc.MysqlStreamProvider#getInputStream()
     */
    @Override
    public InputStream getInputStream() {
        return inputStream;
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.client.jdbc.MysqlStreamProvider#getOutputStream()
     */
    @Override
    public OutputStream getOutputStream() {
        return outputStream;
    }

    public Object getConnection() {
        return connection;
    }
}
