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

package com.github.mysqlbinlog.executor.context;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import com.github.mysqlbinlog.executor.metadata.MetadataHolder;

public class SimpleExecutorContextImpl implements ExecutorContext {
    private Connection connection;
    private MetadataHolder metadataHolder;
    private Map<String, Object> values;

    public SimpleExecutorContextImpl() {
        this.values = new HashMap<String, Object>();
    }

    public SimpleExecutorContextImpl(Connection connection, MetadataHolder metadataHolder) {
        this.values = new HashMap<String, Object>();
        this.connection = connection;
        this.metadataHolder = metadataHolder;
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.context.ApplierContext#getConnection()
     */
    @Override
    public Connection getConnection() {
        return connection;
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.context.ApplierContext#getMetadataHolder()
     */
    @Override
    public MetadataHolder getMetadataHolder() {
        return metadataHolder;
    }

    public void setMetadataHolder(MetadataHolder metadataHolder) {
        this.metadataHolder = metadataHolder;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.context.ApplierContext#setValue(java.lang.String, java.lang.Object)
     */
    @Override
    public void setValue(String name, Object value) {
        this.values.put(name, value);
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.context.ApplierContext#getValue(java.lang.String)
     */
    @Override
    public Object getValue(String name) {
        return this.values.get(name);
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlog.applier.context.ApplierContext#resetValues()
     */
    @Override
    public void resetValues() {
        this.values.clear();
    }
}
