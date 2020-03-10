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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.sql.Statement;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mysqlbinlog.model.event.AnonymousGtidEvent;
import com.github.mysqlbinlog.model.event.BinlogEvent;
import com.github.mysqlbinlog.model.event.DeleteRowsEvent;
import com.github.mysqlbinlog.model.event.FormatDescriptionEvent;
import com.github.mysqlbinlog.model.event.QueryEvent;
import com.github.mysqlbinlog.model.event.TableMapEvent;
import com.github.mysqlbinlog.model.event.UpdateRowsEvent;
import com.github.mysqlbinlog.model.event.WriteRowsEvent;
import com.github.mysqlbinlog.model.event.XidEvent;
import com.github.mysqlbinlog.model.event.extra.Column;
import com.github.mysqlbinlog.model.event.extra.Pair;
import com.github.mysqlbinlog.model.event.extra.Row;
import com.github.mysqlbinlogreader.common.exception.RuntimeMysqlBinlogClientException;


public class MysqlBinlogReaderJdbcImplIT {
    private final static Logger logger = LoggerFactory.getLogger(MysqlBinlogReaderJdbcImplIT.class);

    private String ddlStart[];
    private String ddlEnd[];
    private String dml[];

    private Properties prop;

    private ConnectionJdbcImpl creatorEventConnection; 


    public class EventCreatorTask implements Callable<Integer> {

        @Override
        public Integer call() {
            int result = 1;

            try {
                java.sql.Connection jdbcConnection = creatorEventConnection.getConnection();
                Statement statement = jdbcConnection.createStatement();

                for (int i = 0; i < dml.length; i++) {
                    if (dml[i] != null && dml[i].trim().length() > 0) {
                        logger.debug("[{}]", dml[i].trim());
                        statement.execute(dml[i]);
                    }
                }

                for (int i = 0; i < ddlEnd.length; i++) {
                    if (ddlEnd[i] != null && ddlEnd[i].trim().length() > 0) {
                        logger.debug("[{}]", ddlEnd[i].trim());
                        statement.execute(ddlEnd[i]);
                    }
                }

                statement.close();

            } catch (Exception e) {
                result = 0;
                logger.error("MysqlClient Error [" + e.getMessage() + "]", e);
            } 

            return result;
        }

    }

    public Properties loadProperties(String file) { 
        Properties prop = new Properties();

        try (InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(file)) {
            prop.load(resourceAsStream);
        } catch (Exception e) {
            logger.error("Unable to load properties file[{}]", file, e);
        }

        return prop;
    }

    @Before
    public void init() throws Exception {

        String currentProfile = System.getProperty("profileId");
        logger.debug("CurrentProfile [{}]", currentProfile);

        this.prop = loadProperties((currentProfile == null? "test-unit.properties" : "test.properties"));

        for (Entry<Object, Object> entry : prop.entrySet()) {
            logger.debug("Prop [{}]=[{}]", entry.getKey(), entry.getValue());
        }

        Properties sqls = loadProperties("sql.properties");

        String ddlStartProp = (String) sqls.get("ddl_start");
        this.ddlStart = ddlStartProp.split(";");

        String ddlEndProp = (String) sqls.get("ddl_end");
        this.ddlEnd = ddlEndProp.split(";");

        String dmlProp = (String) sqls.get("dml");
        this.dml = dmlProp.split(";"); 


        //init database
        creatorEventConnection = new ConnectionJdbcImpl(
                prop.getProperty("db.connectionUrl", "jdbc:mysql://localhost:33065"), 
                prop.getProperty("db.username", "testuser"), 
                prop.getProperty("db.password", "test"));        
        creatorEventConnection.setJdbcDriverClassName(prop.getProperty("db.driverClassName", "com.mysql.jdbc.Driver"));
        creatorEventConnection.connect();

        java.sql.Connection jdbcConnection = creatorEventConnection.getConnection();
        Statement statement = jdbcConnection.createStatement();
        for (int i = 0; i < ddlStart.length; i++) {
            if (ddlStart[i] != null && ddlStart[i].trim().length() > 0) {
                logger.debug("[{}]", ddlStart[i].trim());
                statement.execute(ddlStart[i]);
            }
        }
        statement.close();
    }

    @After
    public void clean() {
        creatorEventConnection.close();
    }


    private void verifyFormatDescriptionEvent(FormatDescriptionEvent event) {
        assertTrue(event.getBinlogVersion() > 0);
        assertNotNull(event.getServerVersion());
    }

    private void verifyQueryEvent(QueryEvent event) {
        assertNotNull(event.getDatabaseName());
        assertNotNull(event.getSql());
        assertTrue(event.getStatusVariables().size() > 0);
        assertTrue(event.getThreadId() > 0);
    }

    private void verifyTableMapEvent(TableMapEvent event) {
        assertNotNull(event.getDatabaseName());
        assertNotNull(event.getTableName());
        assertTrue(event.getTableId() > 0);
    }

    private void verifyWriteRowsEventV2(WriteRowsEvent event) {
        assertNotNull(event.getDatabaseName());
        assertNotNull(event.getTableName());
        assertTrue(event.getTableId() > 0);
        assertTrue(event.getRows().size() > 0);
        assertTrue(event.getColumnCount() > 0);
        for (Row row : event.getRows()) {
            assertTrue(row.getColumns().size() > 0);
            for (Column col : row.getColumns()) {
                assertNotNull(col.getName());
                assertTrue(col.getType() > 0);
                assertNotNull(col.getValue());
            }
        }
    }

    private void verifyUpdateRowsEvent(UpdateRowsEvent event) {
        assertNotNull(event.getDatabaseName());
        assertNotNull(event.getTableName());
        assertTrue(event.getTableId() > 0);
        assertTrue(event.getRows().size() > 0);
        assertTrue(event.getColumnCount() > 0);

        for (Pair<Row> pair : event.getRows()) {
            for (Row row : new Row[] {pair.getBefore(), pair.getAfter()}) {
                assertTrue(row.getColumns().size() > 0);
                for (Column col : row.getColumns()) {
                    assertNotNull(col.getName());
                    assertTrue(col.getType() > 0);
                    assertNotNull(col.getValue());
                }
            }
        }
    }

    private void verifyDeleteRowsEvent(DeleteRowsEvent event) {
        assertNotNull(event.getDatabaseName());
        assertNotNull(event.getTableName());
        assertTrue(event.getTableId() > 0);
        assertTrue(event.getRows().size() > 0);
        assertTrue(event.getColumnCount() > 0);
        for (Row row : event.getRows()) {
            assertTrue(row.getColumns().size() > 0);
            for (Column col : row.getColumns()) {
                assertNotNull(col.getName());
                assertTrue(col.getType() > 0);
                assertNotNull(col.getValue());
            }
        }
    }

    private void verifyXidEvent(XidEvent event) {
        assertTrue(event.getXid() > 0);
    }


    private void verifyAnonymousGtidEvent(AnonymousGtidEvent event) {
        assertNotNull(event);
    }

    private void verifyEvent(BinlogEvent event) {
        assertNotNull(event.getHeader());
        assertTrue(event.getHeader().getServerId() > 0);
        if (event instanceof FormatDescriptionEvent) {
            verifyFormatDescriptionEvent((FormatDescriptionEvent)event);
        } else if (event instanceof QueryEvent) {
            verifyQueryEvent((QueryEvent)event);
        } else if (event instanceof TableMapEvent) {
            verifyTableMapEvent((TableMapEvent)event);
        } else if (event instanceof WriteRowsEvent) {
            verifyWriteRowsEventV2((WriteRowsEvent)event);
        } else if (event instanceof UpdateRowsEvent) {
            verifyUpdateRowsEvent((UpdateRowsEvent)event);
        } else if (event instanceof DeleteRowsEvent) {
            verifyDeleteRowsEvent((DeleteRowsEvent)event);
        } else if (event instanceof XidEvent) {
            verifyXidEvent((XidEvent)event);
        } else if (event instanceof AnonymousGtidEvent) {
            verifyAnonymousGtidEvent((AnonymousGtidEvent)event);
        } 
    }

    @Test
    public void predefinedSqlTest() throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        Future<Integer> mysqlClientFuture = null;
        try {
            ConnectionJdbcImpl connection = new ConnectionJdbcImpl(
                    prop.getProperty("db.connectionUrl", "jdbc:mysql://localhost:33065"), 
                    prop.getProperty("db.username", "testuser"), 
                    prop.getProperty("db.password", "test"));        
            connection.setJdbcDriverClassName(prop.getProperty("db.driverClassName", "com.mysql.jdbc.Driver"));
            connection.connect();
            MysqlBinlogReaderJdbcImpl mysqlBinlogReader = new MysqlBinlogReaderJdbcImpl();

            mysqlBinlogReader.setConnection(connection);
            mysqlBinlogReader.setServerId(12345);
            mysqlBinlogReader.open();

            mysqlClientFuture = executor.submit(new EventCreatorTask());

            while (true) {
                BinlogEvent event = mysqlBinlogReader.readBinlogEvent();

                if (event == null) {
                    throw new RuntimeMysqlBinlogClientException("Null Event");
                }

                logger.debug("EVENT [{}]", event);

                verifyEvent(event);
                
                if (event instanceof QueryEvent) {
                    if (((QueryEvent)event).getSql().startsWith("DROP DATABASE mbr_test")) {
                        break;
                    }
                }
            }

            mysqlBinlogReader.close();

        } catch (Exception e) {
            logger.error("MysqlBinlogReader Error [" + e.getMessage() + "]", e);
        } finally  {
            if (mysqlClientFuture != null) {
                Integer mysqlClientResult = mysqlClientFuture.get();
                logger.debug("Mysql Init Script result [{}]", mysqlClientResult);
            }
        }
    }
}
