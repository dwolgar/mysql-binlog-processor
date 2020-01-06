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

import com.github.mysqlbinlog.model.event.WriteRowsEvent;
import com.github.mysqlbinlog.model.event.extra.Column;
import com.github.mysqlbinlog.model.event.extra.Row;
import com.github.mysqlbinlog.executor.metadata.ColumnDescription;
import com.github.mysqlbinlog.executor.metadata.TableDescription;
import com.github.mysqlbinlog.transaction.context.ExecutorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class WriteRowsEventTypeExecutor extends AbstractRowEventTypeExecutor<WriteRowsEvent> {
    private static final Logger logger = LoggerFactory.getLogger(WriteRowsEventTypeExecutor.class);
    
    private static final String FIELD_COLUMNS_NAME = "fieldColumns";
    
    public WriteRowsEventTypeExecutor() {
    }
    
    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.AbstractRowEventApplier#generateEventSql(ApplierContext, BinlogEvent)
     */
    @Override
    protected String buildEventSql(ExecutorContext context, WriteRowsEvent event) {
        TableDescription tableDesc = context.getMetadataHolder().getTableMetaData(event.getDatabaseName(), event.getTableName());
        Collection<ColumnDescription> fieldColumns = tableDesc.getColumnDescriptions();
        context.setValue(FIELD_COLUMNS_NAME, fieldColumns);

        StringBuffer sqlStringBuffer = new StringBuffer();
        sqlStringBuffer.append("INSERT INTO ")
            .append(event.getDatabaseName())
            .append(".")
            .append(event.getTableName())
            .append("(")
            .append(String.join(", ", fieldColumns.stream().map(column -> column.getName()).collect(Collectors.toList())))
            .append(") VALUES (")
            .append(String.join(", ", Collections.nCopies(fieldColumns.size(), "?")))
            .append(")");
        
        return sqlStringBuffer.toString();
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.AbstractRowEventApplier#prepareBatch(PreparedStatement, ApplierContext, BinlogEvent)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void prepareBatch(ExecutorContext context, WriteRowsEvent event) {
        Collection<ColumnDescription> fieldColumns = (Collection<ColumnDescription>) context.getValue(FIELD_COLUMNS_NAME);
        

        for (Row row : event.getRows()) {
            int parameterIndex = 1;
            for (ColumnDescription column : fieldColumns) {
                Column columnData = row.getColumns().get(column.getIndex() - 1);
                logger.debug("DATA [" + column.getIndex() + "][" + parameterIndex + "][" + columnData.getName() + "][" + columnData.getValue() + "]");
                this.setParameter(context, parameterIndex++, columnData.getValue());
            }
            
            this.addBatch(context);
        }
    }

}
