package com.github.mysqlbinlog.transaction.aggregator;

import com.github.mysqlbinlog.model.event.BinlogEvent;

public interface AggregatorContext {
    public void setState(AggregatorState state);

    public AggregatorState getState(); 

    public void addEvent(BinlogEvent event);

    public void sendEvents();

    public void clearEvents();
}
