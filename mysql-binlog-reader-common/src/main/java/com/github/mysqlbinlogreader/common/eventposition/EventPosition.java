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
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((binlogFileName == null) ? 0 : binlogFileName.hashCode());
        result = prime * result + (int) (position ^ (position >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EventPosition other = (EventPosition) obj;
        if (binlogFileName == null) {
            if (other.binlogFileName != null)
                return false;
        } else if (!binlogFileName.equals(other.binlogFileName))
            return false;
        if (position != other.position)
            return false;
        return true;
    }

}
