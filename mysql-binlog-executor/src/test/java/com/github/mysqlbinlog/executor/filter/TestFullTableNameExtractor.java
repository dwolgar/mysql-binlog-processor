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

package com.github.mysqlbinlog.executor.filter;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class TestFullTableNameExtractor {
    private final Logger logger = LoggerFactory.getLogger(TestFullTableNameExtractor.class);
    
    private QueryEventFullTableNameExtractor queryEventFullTableNameExtractor;
    
    @Before
    public void init() {
        this.queryEventFullTableNameExtractor = new QueryEventFullTableNameExtractor();
    }
    
    @Test
    public void testQueryEventFullTableNameExtractor() {
        String[] sqls = new String[] {
            "DELETE from tablename",
            "DELETE from `dbname`.\"tablename\"",
            "DELETE from tablename WHERE field1=1",
            "DELETE from `dbname`.tablename WHERE field1=1",
            "insert into dbname.\"tablename\" (field1, field2, field3) VALUES (1, 2, 3)",
            "INSERT INTO dbname.'tablename' (field1, field2, field3) SELECT * FROM table2",
            "INSERT INTO tablename (field1, field2, field3) SELECT * FROM table2",
            "INSERT INTO tablename(field1, field2, field3) SELECT * FROM table2",
            "UPDATE dbname.tablename SET field1=1 WHERE field2=2",
            "UPDATE tablename SET field1=1",
            "UPDATE dbname.tablename, dbname.tablename2 SET field1=1 WHERE field2=2",
            "UPDATE dbname.tablename, dbname.tablename2 SET field1=1",
            "UPDATE dbname.tablename, tablename2, dbname.tablename3 SET field1=1",
            "UPDATE tablename, dbname.tablename2, dbname.tablename3, dbname.tablename4\n SET field1=1",
            "REPLACE INTO global.activeSubscriptionsCreditCard(field1,\n field2,\n field3)\n VALUES (1, 2, 3)",
            "REPLACE INTO global.activeSubscriptionsCreditCard(\n"
                    + "                                                                    subscriptionId,\n"
                    + "                                                                    cardId,\n"
                    + "                                                                    expId,\n"
                    + "                                                                    system)\n"
                    + "                                                            VALUES('0917221301000000498',\n"
                    + "                                                                   '3',\n"
                    + "                                                                   '272',\n"
                    + "                                                                   '2')",
        };
        
        for (String sql : sqls) {
            String fullTableName = queryEventFullTableNameExtractor.extractTableNameFromSql(sql);
            logger.debug("SQL [" + sql + "][" + fullTableName + "]");
        }
    
        String temp = "TEST31'COOL'";
        String r1 = temp.replaceAll("'", "\\\\'");
        String r2 = temp.replaceAll("\'", "\\'");
        
        logger.debug("[" + temp + "][" + r1 + "][" + r2 + "]");
    }
    
    
}
