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

package com.github.mysqlbinlog.executor.jdbc;

import com.github.mysqlbinlog.executor.jdbc.EventTypeExecutor;
import com.github.mysqlbinlog.executor.jdbc.EventTypeExecutorFactory;
import com.github.mysqlbinlog.executor.jdbc.context.ExecutorContext;
import com.github.mysqlbinlog.executor.jdbc.context.SimpleExecutorContextImpl;
import com.github.mysqlbinlog.executor.jdbc.metadata.MetadataHolder;
import com.github.mysqlbinlog.executor.jdbc.metadata.SimpleMetadataHolderImpl;
import com.github.mysqlbinlog.model.event.BinlogEvent;
import com.github.mysqlbinlog.transaction.aggregator.TransactionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.List;

public class SimpleJdbcExecutorTransactionHandlerImpl implements TransactionHandler {
    private static final Logger logger = LoggerFactory.getLogger(SimpleJdbcExecutorTransactionHandlerImpl.class);

    private EventTypeExecutorFactory eventExecutorFactory;
    
    MetadataHolder metadataHolder;
    ExecutorContext context;
    
    private Connection connection;
    private String jdbcDriverName; 
    private String connectionString;
    private String username;
    private String password;


    public SimpleJdbcExecutorTransactionHandlerImpl() {
        this.eventExecutorFactory = new EventTypeExecutorFactory();
    }
    
    public void connect() {
        try {
            Driver driver = (Driver) Class.forName(this.jdbcDriverName).newInstance();
            DriverManager.registerDriver(driver);
            
            this.connection = DriverManager.getConnection(connectionString, username, password);
            
            this.metadataHolder = new SimpleMetadataHolderImpl(connection);
            this.context = new SimpleExecutorContextImpl(connection, metadataHolder);

        } catch (Exception e) {
            logger.error("ERROR [" + e.getMessage() + "]", e);
        }
    }
    
    public void disconnect() { 
        try {
            this.connection.close();
        } catch (Exception e) {
            logger.error("ERROR [" + e.getMessage() + "]", e);
        }
    }




    private void startTransaction() {
        try {
            if (this.connection.getAutoCommit()) {
                this.connection.setAutoCommit(false);
            }
            
        } catch (Exception e) {
            logger.error("ERROR [" + e.getMessage() + "]", e);
            throw new RuntimeException(e);
        }
    }
    
    private void rollbackTransaction() {
        try {
            this.connection.rollback();
        } catch (Exception e) {
            logger.error("ERROR [" + e.getMessage() + "]", e);
            throw new RuntimeException(e);
        }
    }

    private void commitTransaction() {
        try {
            this.connection.commit();
        } catch (Exception e) {
            logger.error("ERROR [" + e.getMessage() + "]", e);
            throw new RuntimeException(e);
        }
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.Applier#apply(java.util.List)
     */
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public boolean handle(List<BinlogEvent> events) {
        try {
            startTransaction();
            for (BinlogEvent event : events) {
                EventTypeExecutor eventExecutor = eventExecutorFactory.getEventExecutor(event);
                eventExecutor.execute(context, event);
            }
            commitTransaction();
        } catch (Exception e) {
            logger.error("ERROR [" + e.getMessage() + "]", e);
            rollbackTransaction();
            throw new RuntimeException(e);
        }
        
        return true;
    }



    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
    
    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

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

    public String getJdbcDriverName() {
        return jdbcDriverName;
    }

    public void setJdbcDriverName(String jdbcDriverName) {
        this.jdbcDriverName = jdbcDriverName;
    }

}
