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

package com.github.mysqlbinlog.transaction;

import com.github.mysql.constant.MysqlConstants;
import com.github.mysqlbinlog.model.event.BinlogEvent;
import com.github.mysqlbinlog.model.event.QueryEvent;
import com.github.mysqlbinlog.model.variable.QSQLModeCode;
import com.github.mysqlbinlog.model.variable.StatusVariable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class SqlModeBasedFilterTransactionHandlerImpl implements TransactionHandler {
    private static final Logger logger = LoggerFactory.getLogger(SqlModeBasedFilterTransactionHandlerImpl.class);
    
    private long deniedSqlModes;
    
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
        
        Iterator<BinlogEvent> it = events.iterator();
        while (it.hasNext()) {
            BinlogEvent event = it.next();
            if (event.getHeader().getEventType() == MysqlConstants.QUERY_EVENT && !this.isQueryAllowed((QueryEvent) event)) {
                it.remove();
                if (logger.isDebugEnabled()) {
                    logger.debug("QUERY [" + event + "] SKIPPED");
                }
            }
        }
        
        return true;
    }

    public long getDeniedSqlModes() {
        return deniedSqlModes;
    }

    public void setDeniedSqlModes(long deniedSqlModes) {
        this.deniedSqlModes = deniedSqlModes;
    }
    
    public void setDeniedSqlModes(String deniedSqlModes) {
        if (deniedSqlModes == null || deniedSqlModes.length() <= 0) {
            return;
        }
        
        Set<Entry<Long, String>> sqlModes = MysqlConstants.sqlModes.entrySet();
        
        String[] modes = deniedSqlModes.split("\\s*|\\s*");
        for (String mode : modes) {
            for (Entry<Long, String> entry : sqlModes) {
                if (entry.getValue().equalsIgnoreCase(mode)) {
                    this.deniedSqlModes = this.deniedSqlModes | entry.getKey();
                    break;
                }
            }
        }
    }

}
