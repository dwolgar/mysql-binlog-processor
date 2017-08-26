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

import com.github.mysql.constant.MysqlConstants;
import com.github.mysqlbinlog.executor.filter.FullTableNameExtractor;
import com.github.mysqlbinlog.executor.filter.QueryEventFullTableNameExtractor;
import com.github.mysqlbinlog.executor.filter.RowEventFullTableNameExtractor;
import com.github.mysqlbinlog.model.event.BinlogEvent;
import com.github.mysqlbinlog.model.event.QueryEvent;
import com.github.mysqlbinlog.transaction.aggregator.TransactionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class TableNameBasedFilterTransactionHandlerImpl implements TransactionHandler {
    private static final Logger logger = LoggerFactory.getLogger(TableNameBasedFilterTransactionHandlerImpl.class);
    
    private String fileName;
    private Set<String> allowedTables;
    
    private Map<Integer, FullTableNameExtractor<? extends BinlogEvent>> fullTableNameExtractors;
    
    private boolean skipNonDmEvents;

    public TableNameBasedFilterTransactionHandlerImpl() {
        this.allowedTables = new TreeSet<>();
        this.fullTableNameExtractors = new HashMap<Integer, FullTableNameExtractor<? extends BinlogEvent>>();
        this.fullTableNameExtractors.put(MysqlConstants.QUERY_EVENT, new QueryEventFullTableNameExtractor());
        this.fullTableNameExtractors.put(MysqlConstants.UPDATE_ROWS_EVENT, new RowEventFullTableNameExtractor());
        this.fullTableNameExtractors.put(MysqlConstants.UPDATE_ROWS_EVENT_V2, new RowEventFullTableNameExtractor());
        this.fullTableNameExtractors.put(MysqlConstants.WRITE_ROWS_EVENT, new RowEventFullTableNameExtractor());
        this.fullTableNameExtractors.put(MysqlConstants.WRITE_ROWS_EVENT_V2, new RowEventFullTableNameExtractor());
        this.fullTableNameExtractors.put(MysqlConstants.DELETE_ROWS_EVENT, new RowEventFullTableNameExtractor());
        this.fullTableNameExtractors.put(MysqlConstants.DELETE_ROWS_EVENT_V2, new RowEventFullTableNameExtractor());
    }
    
    public void loadTables() {
        try {
            String position = new String(Files.readAllBytes(Paths.get(fileName)));
            String [] parts = position.split("\\r?\\n");
            if (parts == null) {
                return;
            }
            
            for (String tableName : parts) {
                this.allowedTables.add(tableName);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    
    }
    
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected boolean filterOut(BinlogEvent event) {
        if (this.skipNonDmEvents) {
            if (!this.fullTableNameExtractors.containsKey(event.getHeader().getEventType())) {
                return true;
            }
        }
        
        FullTableNameExtractor fullTableNameExtractor = this.fullTableNameExtractors.get(event.getHeader().getEventType());
        if (fullTableNameExtractor == null) {
            return false;
        }
        
        String fullTableName = fullTableNameExtractor.extractFullTableName(event);
        if (fullTableName == null) {
            return false;
        }
        
        if (allowedTables.contains(fullTableName)) {
            return false;
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("TABLE [" + fullTableName + "] SKIPPED");
            }
            return true;
        }
        
    }
    
    protected boolean isTransactionEmpty(List<BinlogEvent> events) {
        if (events.size() == 2) {
            BinlogEvent first = events.get(0);
            BinlogEvent last = events.get(1);
            
            if (first.getHeader().getEventType() == MysqlConstants.QUERY_EVENT && "BEGIN".equalsIgnoreCase(((QueryEvent)first).getSql())  
                && (last.getHeader().getEventType() == MysqlConstants.XID_EVENT  
                    || (last.getHeader().getEventType() == MysqlConstants.QUERY_EVENT && "COMMIT".equalsIgnoreCase(((QueryEvent)first).getSql())))) {
                
                return true;
            }
        }
        
        return false;
    }
    
    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.transaction.TransactionHandler#handle(java.util.List)
     */
    @Override
    public boolean handle(List<BinlogEvent> events) {
        if (events == null || events.size() <= 0) {
            return false;
        }
        
        Iterator<BinlogEvent> it = events.iterator();
        while (it.hasNext()) {
            if (filterOut(it.next())) {
                it.remove();
            }
        }
        
        if (this.isTransactionEmpty(events)) {
            events.clear();
        }
        
        return (events.size() > 0 ? true : false);
    }

    
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public boolean isSkipNonDmEvents() {
        return skipNonDmEvents;
    }

    public void setSkipNonDmEvents(boolean skipNonDmEvents) {
        this.skipNonDmEvents = skipNonDmEvents;
    }

}
