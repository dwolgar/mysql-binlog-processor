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

package com.github.mysqlbinlogreader.common.eventposition;

public class EventPosition {
    private String binlogFileName;
    private long position;
    
    public EventPosition(String binlogFileName, long position) {
        this.binlogFileName = binlogFileName;
        this.position = position;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }
    
    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }
    
    public long getPosition() {
        return position;
    }
    
    public void setPosition(long position) {
        this.position = position;
    }
    
    @Override
    public String toString() {
        return "[" + this.binlogFileName + ":" + this.position + "]";
    }
}
