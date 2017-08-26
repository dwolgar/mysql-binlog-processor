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

package com.github.mysqlbinlog.executor.jdbc.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

public class TableDescription {
    private final String databaseName;
    private final String tableName;
    private final TreeMap<Integer, ColumnDescription> columnDescriptions;
    
    public TableDescription(String databaseName, String tableName, TreeMap<Integer, ColumnDescription> columnDescriptions) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columnDescriptions = columnDescriptions;
    }

    public Collection<ColumnDescription> getColumnDescriptions() {
        return columnDescriptions.values();
    }
    
    public Collection<ColumnDescription> getPrimaryKeys() {
        List<ColumnDescription> pkList = new ArrayList<ColumnDescription>();
        for (ColumnDescription columnDescription : columnDescriptions.values()) {
            if (columnDescription.isPrimaryKey()) {
                pkList.add(columnDescription);
            }
        }
        return pkList;
    }


    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }
}
