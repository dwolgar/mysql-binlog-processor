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

import com.github.mysql.constant.MysqlConstants;
import com.github.mysqlbinlog.model.event.QueryEvent;
import com.github.mysqlbinlog.model.variable.QAutoIncrement;
import com.github.mysqlbinlog.model.variable.QCharsetCode;
import com.github.mysqlbinlog.model.variable.QFlags2Code;
import com.github.mysqlbinlog.model.variable.QSQLModeCode;
import com.github.mysqlbinlog.model.variable.StatusVariable;
import com.github.mysqlbinlog.transaction.context.ExecutorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class QueryEventTypeExecutor implements EventTypeExecutor<QueryEvent> {
    private static final Logger logger = LoggerFactory.getLogger(QueryEventTypeExecutor.class);

    private static final Pattern DATABASE_DDL_PATTERN = 
            Pattern.compile("^\\s*((CREATE\\s+DATABASE)|(CREATE\\s+SCHEMA)|(DROP\\s+DATABASE)|(DROP\\s+SCHEMA)).+", 
                    Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    private String currentDatabaseName;
    private long currentTimestamp;

    private boolean isAutocommit;

    private List<String> sqlStatements = new ArrayList<String>();


    public QueryEventTypeExecutor() {
    }

    private boolean useSetDatabase(String sql) {

        Matcher matcher = DATABASE_DDL_PATTERN.matcher(sql);
        if (matcher.matches()) {
            return false;
        } else {
            return true;
        }

    }

    private void executeContext(ExecutorContext context) {
        Connection connection = context.getConnection();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            for (String sql : sqlStatements) {
                statement.addBatch(sql);
            }
            int[] result = statement.executeBatch();
            logger.info("BATCH RESULT [" + Arrays.toString(result) + "]");

        } catch (Exception e) {
            logger.error("ERROR [" + e.getMessage() + "]", e);
            throw new RuntimeException(e);
        } finally {
            this.sqlStatements.clear();
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                    logger.error("ERROR [" + e.getMessage() + "]", e);
                }
            }
        }
    }

    private String getSqlChangeDatabaseName(String dbName) {
        return "USE " + dbName;
    }

    private void setDatabase(String dbName) {
        if (dbName != null && dbName.length() > 0 && !dbName.equals(this.currentDatabaseName)) {
            this.currentDatabaseName = dbName;
            String sql = getSqlChangeDatabaseName(dbName);
            if (sql != null && sql.length() > 0) {
                logger.debug("SQL in batch [" + sql + "]");
                sqlStatements.add(sql);
            }
        }
    }

    private void setStatusVariables(List<StatusVariable> variables) {
        if (variables == null) {
            return;
        }

        for (StatusVariable var : variables) {
            if (var instanceof QFlags2Code) {
                QFlags2Code temp = (QFlags2Code) var;
                int flags = temp.getFlags();
                this.isAutocommit = (flags & MysqlConstants.OPTION_NOT_AUTOCOMMIT) == 0;
                boolean isAutoisnull = (flags & MysqlConstants.OPTION_AUTO_IS_NULL) > 0;
                boolean isNoForeignKeyChecks  = (flags & MysqlConstants.OPTION_NO_FOREIGN_KEY_CHECKS) > 0;
                boolean isRelaxedUniqueChecks = (flags & MysqlConstants.OPTION_RELAXED_UNIQUE_CHECKS) > 0;

                this.sqlStatements.add(                            
                        "SET @@session.autocommit=" + (isAutocommit ? 1 : 0) + ", " 
                                + "@@session.sql_auto_is_null=" + (isAutoisnull ? 1 : 0) + ", "
                                + "@@session.foreign_key_checks=" + (isNoForeignKeyChecks ? 0 : 1) + ", "
                                + "@@session.unique_checks=" + (isRelaxedUniqueChecks ? 0 : 1));
            }

            if (var instanceof QSQLModeCode) {
                QSQLModeCode temp = (QSQLModeCode) var;

                long sqlMode = temp.getSqlMode();
                if (sqlMode != 0L) {

                    List<String> sqlModeList = new ArrayList<String>();
                    for (Long key : MysqlConstants.sqlModes.keySet()) {
                        if ((sqlMode & key) > 0) {
                            sqlModeList.add(MysqlConstants.sqlModes.get(key));
                        }
                    }
                    this.sqlStatements.add(                            
                            "SET @@session.sql_mode='" + String.join(",", sqlModeList) + "'");
                } else {
                    this.sqlStatements.add(                            
                            "SET @@session.sql_mode=''");
                }

            }

            if (var instanceof QAutoIncrement) {
                QAutoIncrement temp = (QAutoIncrement) var;
                this.sqlStatements.add(                            
                        "SET @@session.auto_increment_increment=" + temp.getAutoIncrementIncrement() + ", "
                                + "@@session.auto_increment_offset=" + temp.getAutoIncrementOffset());
            }

            if (var instanceof QCharsetCode) {
                QCharsetCode temp = (QCharsetCode) var;
                this.sqlStatements.add(                            
                        "SET @@session.character_set_client=" + temp.getCharacterSetClient() + ", "
                                + "@@session.collation_connection=" + temp.getCollationConnection() + ", "
                                + "@@session.collation_server=" + temp.getCollationServer());


            }
        }
    }

    private void setTimestamp(long timestamp) {
        if (this.currentTimestamp != timestamp) {
            this.currentTimestamp = timestamp;
            String sql = "SET TIMESTAMP=" + (timestamp / 1000);
            if (sql != null && sql.length() > 0) {
                logger.debug("SQL in batch [" + sql + "]");
                this.sqlStatements.add(sql);
            }
        }
    }

    private void setQuery(String query) {
        logger.debug("SQL in batch [" + query + "]");
        if (!"BEGIN".equalsIgnoreCase(query) && !"COMMIT".equalsIgnoreCase(query) && !"ROLLBACK".equalsIgnoreCase(query)) {
            this.sqlStatements.add(query);
        }
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.EventApplier#apply(java.lang.Object)
     */
    @Override
    public void execute(ExecutorContext context, QueryEvent event) {
        if (this.useSetDatabase(event.getSql())) {
            setDatabase(event.getDatabaseName());
        } else {
            this.currentDatabaseName = null;
        }

        setStatusVariables(event.getStatusVariables());
        setTimestamp(event.getHeader().getTimestamp());
        setQuery(event.getSql());
        executeContext(context);
    }

}
