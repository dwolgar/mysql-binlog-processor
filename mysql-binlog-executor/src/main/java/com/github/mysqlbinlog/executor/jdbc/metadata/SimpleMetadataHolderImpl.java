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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class SimpleMetadataHolderImpl implements MetadataHolder {
    private Map<String, TableDescription> tableCache;
    private Connection connection;
    private DatabaseMetaData metadata;


    public SimpleMetadataHolderImpl() {
        this.tableCache = new HashMap<String, TableDescription>();
    }
    
    public SimpleMetadataHolderImpl(Connection connection) {
        this.tableCache = new HashMap<String, TableDescription>();
        this.connection = connection;
    }


    private TreeMap<Integer, ColumnDescription> getColumnNames(String databaseName, String tableName) {

        TreeMap<Integer, ColumnDescription> columns = new TreeMap<Integer, ColumnDescription>();
        Set<String> primaryKeys = new HashSet<String>();

        try {
            if (this.metadata == null) {
                this.metadata = connection.getMetaData();
            }
            
            ResultSet rs = metadata.getPrimaryKeys(databaseName, null, tableName);
            while ( rs.next() ) {
                String columnName = rs.getString("COLUMN_NAME");
                primaryKeys.add(columnName);
            }
            
            rs.close();

            
            rs = this.metadata.getColumns(databaseName, null, tableName, null);
            while ( rs.next() ) {
                String columnName = rs.getString("COLUMN_NAME"); 
                ColumnDescription cd = new ColumnDescription(
                        columnName, 
                        rs.getString("TYPE_NAME"), 
                        rs.getInt("COLUMN_SIZE"),
                        rs.getInt("ORDINAL_POSITION"),
                        primaryKeys.contains(columnName) ? true : false);
                columns.put(cd.getIndex(), cd);
            } 
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return columns;
    }

    private String getKey(String databaseName, String tableName) {
        return databaseName + "." + tableName;
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.metadata.MetadataHolder#getTableMetaData()
     */
    @Override
    public TableDescription getTableMetaData(String databaseName, String tableName) {
        String key = this.getKey(databaseName, tableName);
        TableDescription table = this.tableCache.get(key);
        if (table == null) {
            TreeMap<Integer, ColumnDescription> columns = this.getColumnNames(databaseName, tableName);
            table = new TableDescription(databaseName, tableName, columns);
            this.tableCache.put(key, table);
        }
        
        return table;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

}
