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

import com.github.mysqlbinlog.model.event.QueryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryEventFullTableNameExtractor implements FullTableNameExtractor<QueryEvent> {
    private static final Logger logger = LoggerFactory.getLogger(QueryEventFullTableNameExtractor.class);
    
    // DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
    // [PARTITION (partition_name,...)]
    // [WHERE where_condition]
    // [ORDER BY ...]
    // [LIMIT row_count]
    private static Pattern DELETE_STM_PATTERN = Pattern.compile(
            "^\\s*DELETE\\s*(?:LOW_PRIORITY)?\\s*(?:QUICK)?\\s*(?:IGNORE)?\\s*(?:FROM)\\s*" 
          + "(?:(?:['`\"]*([a-zA-Z0-9_]+)['`\"]*)\\.){0,1}(?:['`\"]*([a-zA-Z0-9_]+)['`\"]*)\\s*(?:PARTITION)?\\s*(?:WHERE)?\\s*(?:.*)",
            Pattern.CASE_INSENSITIVE);

    // UPDATE [LOW_PRIORITY] [IGNORE] table_reference
    // SET col_name1={expr1|DEFAULT} [, col_name2={expr2|DEFAULT}] ...
    // [WHERE where_condition]
    // [ORDER BY ...]
    // [LIMIT row_count]
    private static Pattern UPDATE_STM_PATTERN = Pattern.compile(
            "^\\s*UPDATE\\s*(?:LOW_PRIORITY\\s*)?(?:IGNORE\\s*)?"
          + "(?:(?:['`\"]*([a-zA-Z0-9_]+)['`\"]*)\\.){0,1}(?:['`\"]*([a-zA-Z0-9_]+)['`\"]*)"
          + "(?:\\s*,\\s*(?:(?:['`\"]*([a-zA-Z0-9_]+)['`\"]*)\\.){0,1}['`\"]*([a-zA-Z0-9_]+)['`\"]*)*\\s+SET\\s+(?:.*)?\\s*(?:WHERE)?\\s*(?:.*)",
            Pattern.CASE_INSENSITIVE);

    // INSERT [LOW_PRIORITY | HIGH_PRIORITY] [IGNORE] [INTO] tbl_name
    // [(col_name,...)] SELECT ...[ ON DUPLICATE KEY UPDATE col_name=expr
    // [,col_name=expr] ... ]
    private static Pattern INSERT_STM_PATTERN = Pattern.compile(
            "^\\s*INSERT\\s*(?:LOW_PRIORITY|HIGH_PRIORITY)?\\s*(?:IGNORE)?\\s*(?:INTO)?\\s*"
          + "(?:(?:['`\"]*([a-zA-Z0-9_]+)['`\"]*\\.){0,1}['`\"]*([a-zA-Z0-9_]+)['`\"]*)[^a-zA-Z0-9_'`\"]+(:?.*)",
            Pattern.CASE_INSENSITIVE);

    //REPLACE [LOW_PRIORITY | DELAYED]
    //        [INTO] tbl_name
    // ...
    private static Pattern REPLACE_STM_PATTERN = Pattern.compile(
            "^\\s*REPLACE\\s*(?:LOW_PRIORITY|DELAYED)?\\s*(?:INTO)?\\s*"
          + "(?:(?:['`\"]*([a-zA-Z0-9_]+)['`\"]*\\.){0,1}['`\"]*([a-zA-Z0-9_]+)['`\"]*)[^a-zA-Z0-9_'`\"]+(:?.*)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    
    private static Pattern[] STM_PATTERNS = {INSERT_STM_PATTERN, DELETE_STM_PATTERN, UPDATE_STM_PATTERN, REPLACE_STM_PATTERN};

    protected String extractTableNameFromSql(String sql) {
        Matcher matcher = null;
        
        boolean found = false;
        for (Pattern pattern : STM_PATTERNS) {
            matcher = pattern.matcher(sql);
            if (matcher.matches()) {
                found = true;
                break;
            }
        }
        
        if (!found) {
            if (logger.isDebugEnabled()) {
                logger.debug("SQL NO PATTERN MATCHED [{}]", sql);
            }
            return null;
        }

        if (logger.isTraceEnabled()) {
            for (int i = 0; i <= matcher.groupCount(); i++) {
                if (logger.isDebugEnabled()) {
                    logger.debug("GROUP [{}][{}]", i, matcher.group(i));
                }
            }
        }
        
        String databaseName = matcher.group(1);
        String tableName = matcher.group(2);
        
        return (databaseName != null && databaseName.length() > 0 ? databaseName + "." + tableName : tableName);
    }
    
    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.executor.filter.FullTableNameExtractor#extractFullTableName(com.github.mysqlbinlog.model.event.BinlogEvent)
     */
    @Override
    public String extractFullTableName(QueryEvent event) {
        String fullTableName = this.extractTableNameFromSql(event.getSql());
        if (fullTableName == null) {
            return null;
        }
        
        if (!fullTableName.contains(".")) {
            fullTableName = event.getDatabaseName() + "." + fullTableName;
        }
        return fullTableName;
    }

}
