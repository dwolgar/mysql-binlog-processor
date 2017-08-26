package com.github.mysqlbinlog.transaction.aggregator;

import com.github.mysqlbinlog.model.event.BinlogEvent;

public interface AggregatorState {
    public boolean process(AggregatorContext context, BinlogEvent event);
}
