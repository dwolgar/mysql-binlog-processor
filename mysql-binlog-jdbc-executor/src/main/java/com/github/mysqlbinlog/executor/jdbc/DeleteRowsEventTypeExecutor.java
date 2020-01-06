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

import com.github.mysqlbinlog.model.event.DeleteRowsEvent;
import com.github.mysqlbinlog.model.event.extra.Column;
import com.github.mysqlbinlog.model.event.extra.Row;
import com.github.mysqlbinlog.executor.metadata.ColumnDescription;
import com.github.mysqlbinlog.executor.metadata.TableDescription;
import com.github.mysqlbinlog.transaction.context.ExecutorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.stream.Collectors;

public class DeleteRowsEventTypeExecutor extends AbstractRowEventTypeExecutor<DeleteRowsEvent> {
    private static final Logger logger = LoggerFactory.getLogger(DeleteRowsEventTypeExecutor.class);
    
    private static final String WHERE_COLUMNS_NAME = "whereColumns";
    
    public DeleteRowsEventTypeExecutor() {
    }
    
    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.AbstractRowEventApplier#buildEventSql(ApplierContext, BinlogEvent)
     */
    @Override
    protected String buildEventSql(ExecutorContext context, DeleteRowsEvent event) {
        TableDescription tableDesc = context.getMetadataHolder().getTableMetaData(event.getDatabaseName(), event.getTableName());
        Collection<ColumnDescription> whereColumns = tableDesc.getPrimaryKeys();
        if (whereColumns.isEmpty()) {
            whereColumns = tableDesc.getColumnDescriptions();
        }
        
        context.setValue(WHERE_COLUMNS_NAME, whereColumns);

        StringBuffer sqlStringBuffer = new StringBuffer();
        sqlStringBuffer.append("DELETE FROM ")
            .append(event.getDatabaseName())
            .append(".")
            .append(event.getTableName())
            .append(" WHERE ")
            .append(String.join(" AND ", whereColumns.stream().map(column -> column.getName() + "=?").collect(Collectors.toList())));
        
        return sqlStringBuffer.toString();
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.AbstractRowEventApplier#prepareBatch(ApplierContext, BinlogEvent)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void prepareBatch(ExecutorContext context, DeleteRowsEvent event) {
        Collection<ColumnDescription> whereColumns = (Collection<ColumnDescription>) context.getValue(WHERE_COLUMNS_NAME);
        
        for (Row row : event.getRows()) {
            int parameterIndex = 1;
            for (ColumnDescription column : whereColumns) {
                Column columnData = row.getColumns().get(column.getIndex() - 1);
                logger.debug(
                        "WHERE DATA [" + column.getIndex() + "][" + parameterIndex + "]"
                      + "[" + columnData.getName() + "][" + columnData.getValue() + "]");
                this.setParameter(context, parameterIndex++, columnData.getValue());
            }

            this.addBatch(context);
        }
    }

}
