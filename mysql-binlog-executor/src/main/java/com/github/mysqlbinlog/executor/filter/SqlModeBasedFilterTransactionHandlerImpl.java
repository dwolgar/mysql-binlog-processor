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
import com.github.mysqlbinlog.model.event.BinlogEvent;
import com.github.mysqlbinlog.model.event.QueryEvent;
import com.github.mysqlbinlog.model.variable.QSQLModeCode;
import com.github.mysqlbinlog.model.variable.StatusVariable;
import com.github.mysqlbinlog.transaction.aggregator.TransactionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class SqlModeBasedFilterTransactionHandlerImpl implements TransactionHandler {
    private static final Logger logger = LoggerFactory.getLogger(SqlModeBasedFilterTransactionHandlerImpl.class);
    
    public static final String SQL_MODE_SAVE_ORIGINAL         = "SET @old_sql_mode = @@session.sql_mode;";
    public static final String SQL_MODE_INCLUDE_DENIED_MODE   = "SET @@session.sql_mode = CONCAT_WS(',', @@session.sql_mode, '%s');";
    public static final String SQL_MODE_RESTORE               = "SET @@session.sql_mode = @old_sql_mode;";

    private String deniedSqlModesString;
    private long deniedSqlModes;
    private boolean addDeniedSqlModes;

    public SqlModeBasedFilterTransactionHandlerImpl() {
        this.deniedSqlModes = 0;
        this.deniedSqlModesString = "";
        this.addDeniedSqlModes = false;
    }
    
    
    protected boolean isQueryAllowed(QueryEvent event) {
        List<StatusVariable> variables = event.getStatusVariables();
        for (StatusVariable variable : variables) {
            if (variable instanceof QSQLModeCode) {
                long sqlMode = ((QSQLModeCode)variable).getSqlMode();
                if ((sqlMode & this.deniedSqlModes) > 0) {
                    return false;
                }
            }
        }
        
        return true;
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.transaction.TransactionHandler#handle(java.util.List)
     */
    @Override
    public boolean handle(List<BinlogEvent> events) {
        if (events == null || events.size() <= 0) {
            return false;
        }
        
        BinlogEvent first = events.get(0);
        if (first.getHeader().getEventType() == MysqlConstants.QUERY_EVENT && "BEGIN".equalsIgnoreCase(((QueryEvent)first).getSql())) {
            if (!this.isQueryAllowed((QueryEvent) first)) {
                return false;
            }
        }
        
        List<BinlogEvent> newEvents = new ArrayList<BinlogEvent>();
        if (this.isAddDeniedSqlModes()) {
            QueryEvent newSqlModeEvent = new QueryEvent(first.getHeader(), null);
            newSqlModeEvent.setSql(SQL_MODE_SAVE_ORIGINAL);
            newEvents.add(newSqlModeEvent);
/*            newSqlModeEvent = new QueryEvent(first.getHeader(), null);
            newSqlModeEvent.setSql(String.format(SQL_MODE_INCLUDE_DENIED_MODE, this.deniedSqlModesString));
            newEvents.add(newSqlModeEvent);
*/        }
                
        for (BinlogEvent event : events) {
            if (event.getHeader().getEventType() == MysqlConstants.QUERY_EVENT && !this.isQueryAllowed((QueryEvent) event)) {
                continue;
            }
            
            if (this.isAddDeniedSqlModes() && event.getHeader().getEventType() == MysqlConstants.QUERY_EVENT) {
                QueryEvent queryEvent = (QueryEvent) event;
                
                List<StatusVariable> statusVariables = queryEvent.getStatusVariables();
                for (int i = 0; i < statusVariables.size(); i++) {
                    StatusVariable variable = statusVariables.get(i);
                    if (variable instanceof QSQLModeCode) {
                        QSQLModeCode newVariable = new QSQLModeCode(((QSQLModeCode) variable).getSqlMode() | this.deniedSqlModes);
                        statusVariables.set(i, newVariable);
                    }
                }
                
                newEvents.add(queryEvent);
            } else {
                newEvents.add(event);
            }
        }
        
        if (this.isAddDeniedSqlModes()) {
            QueryEvent newSqlModeEvent = new QueryEvent(first.getHeader(), null);
            newSqlModeEvent.setSql(SQL_MODE_RESTORE);
            newEvents.add(newSqlModeEvent);
        }
        
        events.clear();
        events.addAll(newEvents);
        
        return true;
    }

    public long getDeniedSqlModes() {
        return deniedSqlModes;
    }

    public void setDeniedSqlModes(long deniedSqlModes) {
        this.deniedSqlModes = deniedSqlModes;
        if (deniedSqlModes == 0) {
            this.deniedSqlModesString = "";
            return;
        } 
        
        List<String> sqlModeList = new ArrayList<String>();
        for (Long key : MysqlConstants.sqlModes.keySet()) {
            if ((this.deniedSqlModes & key) > 0) {
                sqlModeList.add(MysqlConstants.sqlModes.get(key));
            }
        }
        
        this.deniedSqlModesString = String.join(",", sqlModeList);
        
    }
    
    public void setDeniedSqlModes(String deniedSqlModes) {
        if (deniedSqlModes == null || deniedSqlModes.length() <= 0) {
            this.deniedSqlModes = 0;
            this.deniedSqlModesString = "";
            return;
        }
        
        this.deniedSqlModesString = deniedSqlModes;
        
        Set<Entry<Long, String>> sqlModes = MysqlConstants.sqlModes.entrySet();
        
        String[] modes = deniedSqlModes.split("\\s*,\\s*");
        for (String mode : modes) {
            for (Entry<Long, String> entry : sqlModes) {
                if (entry.getValue().equalsIgnoreCase(mode)) {
                    this.deniedSqlModes = this.deniedSqlModes | entry.getKey();
                    break;
                }
            }
        }
    }

    public boolean isAddDeniedSqlModes() {
        return addDeniedSqlModes;
    }

    public void setAddDeniedSqlModes(boolean addDeniedSqlModes) {
        this.addDeniedSqlModes = addDeniedSqlModes;
    }

}
