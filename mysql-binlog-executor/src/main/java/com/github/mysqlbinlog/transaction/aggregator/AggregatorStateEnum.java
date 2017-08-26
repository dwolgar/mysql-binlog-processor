package com.github.mysqlbinlog.transaction.aggregator;

import com.github.mysqlbinlog.model.event.BinlogEvent;
import com.github.mysqlbinlog.model.event.QueryEvent;
import com.github.mysqlbinlog.model.event.XidEvent;

public enum AggregatorStateEnum implements AggregatorState {
    INIT {

        private boolean isDdlStatement(String sql) {
            if (
                sql.startsWith("drop") || sql.startsWith("DROP")
                    || sql.startsWith("create") || sql.startsWith("CREATE")
                    || sql.startsWith("alter") || sql.startsWith("ALTER")) {
                return true;
            } else {
                return false;
            }
        }


        @Override
        public boolean process(AggregatorContext context, BinlogEvent event) {
            if (event instanceof QueryEvent) {
                QueryEvent queryEvent = (QueryEvent) event;
                if ("BEGIN".equalsIgnoreCase(queryEvent.getSql())) {
                    context.addEvent(event);
                    context.setState(COLLECT);
                    return false;
                } else if (this.isDdlStatement(queryEvent.getSql())) {
                    context.addEvent(event);
                    context.sendEvents();
                    return true;
                }
            } 
            return true;
        }

    },

    COLLECT {

        @Override
        public boolean process(AggregatorContext context, BinlogEvent event) {
            context.addEvent(event);

            if (event instanceof XidEvent) {
                context.sendEvents();
                context.setState(INIT);
                return true;
            } else if (event instanceof QueryEvent) {
                QueryEvent queryEvent = (QueryEvent) event;
                if ("COMMIT".equalsIgnoreCase(queryEvent.getSql())) {
                    context.sendEvents();
                    context.setState(INIT);
                    return true;
                } else if ("ROLLBACK".equalsIgnoreCase(queryEvent.getSql())) {
                    context.clearEvents();
                    context.setState(INIT);
                    return true;
                }
            }

            return false;
        }

    };
}
