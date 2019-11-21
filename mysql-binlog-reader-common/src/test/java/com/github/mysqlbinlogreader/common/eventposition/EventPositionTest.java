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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EventPositionTest {
    private static final String testBinlogFileName1 = "binlog_123";
    private static final long testBinlogPosition1 = 125l;
    private static final String testBinlogFileName2 = "binlog_125";
    private static final long testBinlogPosition2 = 127l;
    
    @Before
    public void init() {
    }
    
    @Test
    public void EventPositionGettersTest() {  
        EventPosition eventPosition = new EventPosition(EventPositionTest.testBinlogFileName1, EventPositionTest.testBinlogPosition1);
        assertEquals(EventPositionTest.testBinlogFileName1, eventPosition.getBinlogFileName());
        assertTrue(EventPositionTest.testBinlogPosition1 == eventPosition.getPosition());
    }
    
    @Test
    public void EventPositionSettersTest() {  
        EventPosition eventPosition = new EventPosition(EventPositionTest.testBinlogFileName1, EventPositionTest.testBinlogPosition1);
        assertEquals(EventPositionTest.testBinlogFileName1, eventPosition.getBinlogFileName());
        assertTrue(EventPositionTest.testBinlogPosition1 == eventPosition.getPosition());
        
        eventPosition.setBinlogFileName(EventPositionTest.testBinlogFileName2);
        eventPosition.setPosition(EventPositionTest.testBinlogPosition2);
        
        assertEquals(EventPositionTest.testBinlogFileName2, eventPosition.getBinlogFileName());
        assertTrue(EventPositionTest.testBinlogPosition2 == eventPosition.getPosition());
        
    }

}
