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

package com.github.mysqlbinlog.transaction.aggregator;

import com.github.mysqlbinlog.model.event.BinlogEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TransactionEventAggregator implements AggregatorContext {
    private static final Logger logger = LoggerFactory.getLogger(TransactionEventAggregator.class);

    private List<TransactionHandler> transactionHandlers;

    private List<BinlogEvent> transactionEvents;
    private AggregatorState currentExecutorState;

    public TransactionEventAggregator() {
        this.currentExecutorState = AggregatorStateEnum.INIT;
    }


    @Override
    public void addEvent(BinlogEvent event) {
        if (this.transactionEvents == null) {
            this.transactionEvents = new ArrayList<>();
        }
        this.transactionEvents.add(event);
    }

    @Override
    public void sendEvents() {
        if (this.transactionHandlers == null) {
            return;
        }

        for (TransactionHandler transactionHandler : this.transactionHandlers) {
            if (this.transactionEvents != null && !this.transactionEvents.isEmpty()) {
                if (!transactionHandler.handle(this.transactionEvents)) {
                    break;
                }
            } else {
                break;
            }
        }

        if (this.transactionEvents != null) {
            this.transactionEvents.clear();
        }      
    }

    @Override
    public void clearEvents() {
        if (this.transactionEvents != null) {
            this.transactionEvents.clear();
        }
    }

    @Override
    public void setState(AggregatorState state) {
        this.currentExecutorState = state;
    }

    @Override
    public AggregatorState getState() {
        return this.currentExecutorState;
    }

    public boolean applyEvent(BinlogEvent event) {
        return this.getState().process(this, event);
    }

    public void addTransactionHandler(TransactionHandler transactionHandler) {
        if (this.transactionHandlers == null) {
            this.transactionHandlers = new ArrayList<>();
        }
        this.transactionHandlers.add(transactionHandler);
    }

    public List<TransactionHandler> getTransactionHandlers() {
        return transactionHandlers;
    }

    public void setTransactionHandlers(List<TransactionHandler> transactionHandlers) {
        this.transactionHandlers = transactionHandlers;
    }


}
