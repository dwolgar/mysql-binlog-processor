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

import static org.junit.Assert.assertNull;

import java.sql.DriverManager;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.github.mysqlbinlogreader.common.exception.RuntimeMysqlBinlogClientException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value= {ConnectionJdbcImpl.class})
public class ConnectionJdbcImplTest {
    
    private static final String connectionString = "jdbc:mysql://mysql-hostmame";
    private static final String username = "username";
    private static final String password = "password";

    private ConnectionJdbcImpl connection;

    @Before
    public void init() {
    }
    
    @Test(expected = RuntimeMysqlBinlogClientException.class)
    public void connectTest() throws Exception {
        PowerMockito.mockStatic(DriverManager.class);
        
        connection = new ConnectionJdbcImpl(connectionString, username, password);
        connection.connect();
        assertNull(connection.getInputStream());
        assertNull(connection.getOutputStream());
    }
}
