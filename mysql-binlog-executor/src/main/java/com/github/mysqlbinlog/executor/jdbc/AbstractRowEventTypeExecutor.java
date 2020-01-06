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

import com.github.mysqlbinlog.model.event.BinlogEvent;
import com.github.mysqlbinlog.executor.jdbc.context.ExecutorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;

public abstract class AbstractRowEventTypeExecutor<T extends BinlogEvent> implements EventTypeExecutor<T> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRowEventTypeExecutor.class);

    private static final String PREPARED_STATEMENT_NAME = "preparedStatement";

    protected abstract String buildEventSql(ExecutorContext context, T event);

    protected abstract void prepareBatch(ExecutorContext context, T event);

    protected PreparedStatement prepareStatement(Connection connection, String sql) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            return preparedStatement;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void setParameter(ExecutorContext context, int index, Object value) {
        try {
            PreparedStatement preparedStatement = (PreparedStatement) context.getValue(PREPARED_STATEMENT_NAME);

            preparedStatement.setObject(index, value);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } 
    }

    protected void addBatch(ExecutorContext context) {
        try {
            PreparedStatement preparedStatement = (PreparedStatement) context.getValue(PREPARED_STATEMENT_NAME);

            preparedStatement.addBatch();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } 
    }


    protected int[] executeBatch(PreparedStatement preparedStatement) {
        try {
            int[] result = preparedStatement.executeBatch();
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                preparedStatement.close();
            } catch (Exception e) {
                ;
            }
        }
    }


    protected void closeStatement(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (Exception e) {
                ;
            }
        }
    }

    /* (non-Javadoc)
     * @see EventApplier#apply(ApplierContext, BinlogEvent)
     */
    @Override
    public void execute(ExecutorContext context, T event) {
        String sql = this.buildEventSql(context, event);

        if (logger.isDebugEnabled()) {
            logger.debug("SQL [{}]", sql);
        }

        PreparedStatement preparedStatement = this.prepareStatement(context.getConnection(), sql);

        context.setValue(PREPARED_STATEMENT_NAME, preparedStatement);

        this.prepareBatch(context, event);

        this.executeBatch(preparedStatement);

        context.resetValues();

    }

}
