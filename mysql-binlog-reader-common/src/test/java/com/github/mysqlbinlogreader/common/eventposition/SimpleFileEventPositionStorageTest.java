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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Files.class)
public class SimpleFileEventPositionStorageTest {
    private static final String fileName = "mysql-binlog-position.dat";
    private static final String testBinlogFileName1 = "binlog_123";
    private static final long testBinlogPosition1 = 125l;

    
    @Before
    public void init() throws IOException {
        
    }

    @Test
    public void SimpleFileEventPositionStorageSaveCurrentTest() throws IOException {
        PowerMockito.mockStatic(Files.class);
        
        PowerMockito.when(Files.write(Paths.get(fileName), (testBinlogFileName1 + ":" + testBinlogPosition1).getBytes())).thenReturn(Paths.get(fileName));
        
        PowerMockito.when(Files.readAllBytes(Paths.get(fileName))).thenReturn((testBinlogFileName1 + ":" + testBinlogPosition1).getBytes());

        
        EventPosition eventPosition = new EventPosition(testBinlogFileName1, testBinlogPosition1);
        
        
        SimpleFileEventPositionStorage eventPositionStorage = new SimpleFileEventPositionStorage();
        eventPositionStorage.setFileName(fileName);
        eventPositionStorage.saveCurrent(eventPosition);
        EventPosition eventPositionResult = eventPositionStorage.getCurrent();
        
        assertEquals(eventPositionResult, eventPosition);
    }
}
