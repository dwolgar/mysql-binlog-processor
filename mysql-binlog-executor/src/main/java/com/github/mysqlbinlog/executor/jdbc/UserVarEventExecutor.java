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


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mysqlbinlog.model.event.UserVarEvent;
import com.github.mysqlbinlog.executor.jdbc.context.ExecutorContext;

public class UserVarEventExecutor implements EventTypeExecutor<UserVarEvent> {
    private static final Logger logger = LoggerFactory.getLogger(UserVarEventExecutor.class);

    protected String generateSql(UserVarEvent event) {
        String sql = "SET @" + event.getVarName() + "=?";
        return sql;
    }

    protected void executeContext(ExecutorContext context, String sql, Object value) {
        Connection connection = context.getConnection();
        Statement statement = null;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setObject(1, value);
            preparedStatement.addBatch();
            
            if (logger.isDebugEnabled()) {
                logger.debug("SQL in batch [" + sql + "][" + value + "]");
            }

            int[] result = preparedStatement.executeBatch();
            if (logger.isDebugEnabled()) {
                logger.debug("BATCH RESULT [" + Arrays.toString(result) + "]");
            }

        } catch (Exception e) {
            logger.error("ERROR [" + e.getMessage() + "]", e);
            throw new RuntimeException(e);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (Exception e) {
                    logger.error("ERROR [" + e.getMessage() + "]", e);
                }
            }
        }
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.executor.jdbc.EventTypeExecutor#execute(com.github.mysqlbinlog.executor.jdbc.context.ExecutorContext, com.github.mysqlbinlog.model.event.BinlogEvent)
     */
    @Override
    public void execute(ExecutorContext context, UserVarEvent event) {
        String sql = this.generateSql(event);
        this.executeContext(context, sql, event.getVarValue().getValue());
    }
}
