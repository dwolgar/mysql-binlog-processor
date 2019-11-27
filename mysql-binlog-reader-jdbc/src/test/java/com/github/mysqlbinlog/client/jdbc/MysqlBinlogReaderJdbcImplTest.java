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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mysqlbinlogreader.common.Connection;

@RunWith(JUnit4.class)
public class MysqlBinlogReaderJdbcImplTest {
    private final static Logger logger = LoggerFactory.getLogger(MysqlBinlogReaderJdbcImplTest.class);
    
    private static String jdbcDriverClassName = "com.mysql.cj.jdbc.Driver";
    
    private MysqlBinlogReaderJdbcImpl mysqlBinlogReader;
    
    @Before
    public void init() {
        
        ConnectionJdbcImpl connectionJdbc = new ConnectionJdbcImpl("jdbc:mysql://mysql-hostmame", "username", "password");
        connectionJdbc.setJdbcDriverClassName(jdbcDriverClassName);
        
        mysqlBinlogReader = new MysqlBinlogReaderJdbcImpl();
        mysqlBinlogReader.setConnection(connectionJdbc);
    }

    @Test
    public void initializeConnectionTest() {
        Connection connection = mysqlBinlogReader.initializeConnection();
        assertEquals(connection.getClass().getCanonicalName(), ConnectionJdbcImpl.class.getCanonicalName());
        
    }
}
