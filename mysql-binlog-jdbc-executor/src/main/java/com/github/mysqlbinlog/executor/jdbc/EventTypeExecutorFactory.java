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

import java.util.HashMap;
import java.util.Map;

import com.github.mysqlbinlog.model.event.BinlogEvent;
import com.github.mysqlbinlog.model.event.DeleteRowsEvent;
import com.github.mysqlbinlog.model.event.QueryEvent;
import com.github.mysqlbinlog.model.event.UpdateRowsEvent;
import com.github.mysqlbinlog.model.event.UserVarEvent;
import com.github.mysqlbinlog.model.event.WriteRowsEvent;

@SuppressWarnings("rawtypes")
public class EventTypeExecutorFactory {
    private Map<Class, EventTypeExecutor<? extends BinlogEvent>> classExecuterMap;
    private NopEventExecutor nopEventExecutor;

    public EventTypeExecutorFactory() {
        this.nopEventExecutor = new NopEventExecutor();
        this.classExecuterMap = new HashMap<>();
        this.createDefaultAppliers();
    }
    
    private void createDefaultAppliers() {
        classExecuterMap.put(QueryEvent.class, new QueryEventTypeExecutor());
        classExecuterMap.put(WriteRowsEvent.class, new WriteRowsEventTypeExecutor());
        classExecuterMap.put(DeleteRowsEvent.class, new DeleteRowsEventTypeExecutor());
        classExecuterMap.put(UpdateRowsEvent.class, new UpdateRowsEventTypeExecutor());
        classExecuterMap.put(UserVarEvent.class, new UserVarEventExecutor());
    }
    
    public EventTypeExecutor<? extends BinlogEvent> getEventExecutor(BinlogEvent event) {
        EventTypeExecutor<? extends BinlogEvent> eventExecutor = this.classExecuterMap.get(event.getClass());
        if (eventExecutor == null) {
            eventExecutor = this.nopEventExecutor;
        }
        return eventExecutor;
    }

}
