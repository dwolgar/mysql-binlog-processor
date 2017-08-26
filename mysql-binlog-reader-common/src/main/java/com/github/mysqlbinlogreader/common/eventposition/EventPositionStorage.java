package com.github.mysqlbinlogreader.common.eventposition;

public interface EventPositionStorage {
    public void saveCurrent(EventPosition eventPosition);
    
    public EventPosition getCurrent();
}
