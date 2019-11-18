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

package com.github.mysqlbinlogreader.common;

import com.github.mysqlbinlog.model.event.BinlogEvent;
import com.github.mysqlbinlog.model.event.RotateEvent;
import com.github.mysqlbinlog.model.event.TableMapEvent;
import com.github.mysqlbinlogreader.common.eventposition.EventPosition;
import com.github.mysqlbinlogreader.common.eventposition.EventPositionStorage;
import com.github.mysqlbinlogreader.common.exception.RuntimeMysqlErrorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SimpleMysqlBinlogClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMysqlBinlogClient.class);
            
    private MysqlBinlogReader mysqlBinlogReader;
    private List<MysqlBinlogEventListener> mysqlBinlogEventListeners;
    
    private EventPositionStorage eventPositionStorage;


    public SimpleMysqlBinlogClient() {
        this.mysqlBinlogEventListeners = new ArrayList<>();
    }
    
    private void updateEventPosition() {
        try {
            mysqlBinlogReader.setEventPosition(this.eventPositionStorage.getCurrent());
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Unable to read initial binlog position.\nError[" + e.getMessage() + "]\n"
                           + "Going to use current position from Master");
            }
        }
    }
    
    public void process() {
        try {
            if (this.mysqlBinlogReader.getEventPosition() == null) {
                updateEventPosition();
            }
            mysqlBinlogReader.open();
            while (true) {
                BinlogEvent event = mysqlBinlogReader.readBinlogEvent();
                boolean changePosition = true;
                for (MysqlBinlogEventListener eventListener : this.mysqlBinlogEventListeners) {
                    if (!eventListener.onEvent(event)) {
                        changePosition = false;
                    }
                }
                if (event instanceof RotateEvent) {
                    final RotateEvent e = (RotateEvent) event;
                    if (changePosition) {
                        EventPosition currentEventPosition = mysqlBinlogReader.getEventPosition(); 
                        currentEventPosition.setBinlogFileName(e.getBinlogFileName());
                        currentEventPosition.setPosition(e.getBinlogPosition());
                        eventPositionStorage.saveCurrent(currentEventPosition);
                    }
                } else if (!(event instanceof TableMapEvent)) {
                    long nextPosition = event.getHeader().getNextPosition();
                    if (nextPosition > 0 && changePosition) {
                        EventPosition currentEventPosition = mysqlBinlogReader.getEventPosition(); 
                        currentEventPosition.setPosition(nextPosition);
                        eventPositionStorage.saveCurrent(currentEventPosition);
                    }
                }
            }
        } catch (RuntimeMysqlErrorException ex) {
            LOGGER.error("ERROR [" + ex.getMessage() + "][" + ex.getErrorResponsePacket() + "]", ex);
        } catch (Exception ex) {
            LOGGER.error("ERROR [" + ex.getMessage() + "]", ex);
        } finally {
            mysqlBinlogReader.close();
        }
    }

    
    public void addMysqlBinlogEventListener(MysqlBinlogEventListener listener) {
        this.mysqlBinlogEventListeners.add(listener);
    }
    
    public MysqlBinlogReader getMysqlBinlogReader() {
        return mysqlBinlogReader;
    }
    
    public void setMysqlBinlogReader(MysqlBinlogReader mysqlBinlogReader) {
        this.mysqlBinlogReader = mysqlBinlogReader;
    }
    
    public EventPositionStorage getEventPositionStorage() {
        return eventPositionStorage;
    }
    
    public void setEventPositionStorage(EventPositionStorage eventPositionStorage) {
        this.eventPositionStorage = eventPositionStorage;
    }
}
