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

package com.github.mysqlbinlogreader;

import static org.junit.Assert.assertEquals;

import javax.xml.bind.DatatypeConverter;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mysqlbinlog.event.checksum.Crc32MysqlChecksumImpl;
import com.github.mysqlbinlog.event.deserializer.BinlogEventDeserializerFactory;
import com.github.mysqlbinlog.event.deserializer.BinlogEventFactory;
import com.github.mysqlbinlog.event.deserializer.BinlogEventHeaderDeserializer;
import com.github.mysqlbinlog.event.deserializer.SimpleBinlogDeserializerContextImpl;
import com.github.mysqlbinlog.event.deserializer.SimpleBinlogEventDeserializerFactoryImpl;
import com.github.mysqlbinlog.event.deserializer.SimpleBinlogEventFactoryImpl;
import com.github.mysqlbinlog.event.deserializer.SimpleBinlogEventHeaderDeserializerImpl;
import com.github.mysqlbinlog.event.deserializer.SimpleSingleBinglogEventDeserializerImpl;
import com.github.mysqlbinlog.model.event.BinlogEvent;
import com.github.mysqlbinlog.model.event.FormatDescriptionEvent;
import com.github.mysqlbinlogreader.common.AbstractMysqlBinlogReaderImpl;
import com.github.mysqlbinlogreader.common.MysqlBinlogEventListener;
import com.github.mysqlbinlogreader.common.eventposition.EventPosition;
import com.github.mysqlbinlogreader.common.eventposition.SimpleFileEventPositionStorage;

@RunWith(JUnit4.class)
public class SimpleMysqlBinlogClientTest {
    private static final Logger logger = LoggerFactory.getLogger(SimpleMysqlBinlogClientTest.class);

    private SimpleMysqlBinlogClient mysqlBinlogClient;

    private AbstractMysqlBinlogReaderImpl mysqlBinlogReader;
    private SimpleFileEventPositionStorage eventPositionStorage;

    @Before
    public void init() {
        mysqlBinlogClient = new SimpleMysqlBinlogClient();

        eventPositionStorage = Mockito.mock(SimpleFileEventPositionStorage.class);
        mysqlBinlogReader = Mockito.mock(AbstractMysqlBinlogReaderImpl.class);
        Mockito.doCallRealMethod().when(mysqlBinlogReader).setSingleBinglogEventDeserializer(Mockito.any());
        Mockito.doCallRealMethod().when(mysqlBinlogReader).getSingleBinglogEventDeserializer();
        Mockito.doCallRealMethod().when(mysqlBinlogReader).setBinlogDeserializerContext(Mockito.any());
        Mockito.doCallRealMethod().when(mysqlBinlogReader).getBinlogDeserializerContext();

        BinlogEventHeaderDeserializer binlogEventHeaderParser = new SimpleBinlogEventHeaderDeserializerImpl();
        BinlogEventFactory binlogEventFactory = new SimpleBinlogEventFactoryImpl();
        BinlogEventDeserializerFactory binlogEventDeserializerFactory = new SimpleBinlogEventDeserializerFactoryImpl();

        SimpleSingleBinglogEventDeserializerImpl simpleSingleBinglogEventDeserializer = new SimpleSingleBinglogEventDeserializerImpl();
        simpleSingleBinglogEventDeserializer.setBinlogEventHeaderParser(binlogEventHeaderParser);
        simpleSingleBinglogEventDeserializer.setBinlogEventFactory(binlogEventFactory);
        simpleSingleBinglogEventDeserializer.setBinlogEventUnmarshallerFactory(binlogEventDeserializerFactory);

        mysqlBinlogReader.setSingleBinglogEventDeserializer(simpleSingleBinglogEventDeserializer);

        SimpleBinlogDeserializerContextImpl binlogDeserializerContext = new SimpleBinlogDeserializerContextImpl();
        binlogDeserializerContext.setChecksum(new Crc32MysqlChecksumImpl());
        mysqlBinlogReader.setBinlogDeserializerContext(binlogDeserializerContext);

        mysqlBinlogClient.setEventPositionStorage(eventPositionStorage);
        mysqlBinlogClient.setMysqlBinlogReader(mysqlBinlogReader);
        mysqlBinlogClient.addMysqlBinlogEventListener(new MysqlBinlogEventListener() {
            @Override
            public boolean onEvent(BinlogEvent event) {
                logger.debug("binlog packet [" + event.getHeader().getNextPosition() + "][" + mysqlBinlogReader.getBinlogDeserializerContext().getChecksum().getType() + "]["+ event + "]");
                if (event instanceof FormatDescriptionEvent) {
                    FormatDescriptionEvent formatDescriptionEvent = (FormatDescriptionEvent) event;
                    assertEquals(formatDescriptionEventServerVersion, formatDescriptionEvent.getServerVersion());
                }

                return true;
            }
        });


    }

    @Test 
    public void processTest() {
        BinlogEvent packets[] = new BinlogEvent[hex.length];
        BinlogEvent packet = mysqlBinlogReader.getSingleBinglogEventDeserializer().deserialize(DatatypeConverter.parseHexBinary(hex[0]), mysqlBinlogReader.getBinlogDeserializerContext());

        for (int i = 1; i < hex.length - 1; i++) {
            packets[i - 1] = mysqlBinlogReader.getSingleBinglogEventDeserializer().deserialize(DatatypeConverter.parseHexBinary(hex[i]), mysqlBinlogReader.getBinlogDeserializerContext());
        }
        Mockito.when(mysqlBinlogReader.readBinlogEvent()).thenReturn(packet, packets);
        Mockito.when(mysqlBinlogReader.getEventPosition()).thenReturn(new EventPosition("bin.00004", 4));


        mysqlBinlogClient.process();
    }

    private static final String formatDescriptionEventServerVersion = "8.0.18-0ubuntu0.19.10.1";
    private static final String[] hex = new String[] {
            //FormatDescriptionEvent
            "00AE8CD45D0F01000000780000000000000000000400382E302E31382D307562756E7475302E31392E31302E310000000000000000000000000000000000000000000000000000000000000013000D0008000000000400040000006000041A08000000080808020000000A0A0A2A2A001234000A012163B035",
            //AnonymousGtidEvent
            "00EBD6D55D22010000004D000000E800000000000100000000000000000000000000000000000000000000000002000000000000000001000000000000000558B22ED09705E39238010007FC3124",
            //QueryEvent
            "00EBD6D55D0201000000960000007E010000080011000000000000001200003D000000000000012000A04500000000060373746404FF00FF00FF000C01746573745F62696E6C6F675F7265616465720011500000000000000012FF001400746573745F62696E6C6F675F7265616465720063726561746520646174616261736520746573745F62696E6C6F675F726561646572B8487073",
            //AnonymousGtidEvent
            "009CD7D55D22010000004F000000CD0100000000010000000000000000000000000000000000000000000000000201000000000000000200000000000000987A3E39D09705FC4E019238010079BFA813",
            //QueryEvent
            "009CD7D55D0201000000FF000000CC020000000011000000000000001200003D000000000000012000A04500000000060373746404FF00FF00FF000C01746573745F62696E6C6F675F7265616465720011550000000000000012FF001300746573745F62696E6C6F675F72656164657200435245415445205441424C452060746573745F7461626C656020280A20206069646020696E742831312920756E7369676E6564204E4F54204E554C4C2C0A20206076616C756560206368617228323029204E4F54204E554C4C2064656661756C742027272C0A20205052494D415259204B455920202860696460290A2920454E47494E453D496E6E6F444270FD98F1",
            //AnonymousGtidEvent
            "00D7D7D55D22010000004F0000001B0300000000000000000000000000000000000000000000000000000000000202000000000000000300000000000000A32FC23CD09705FC430192380100FB1EAEC7",
            //QueryEvent
            "00D7D7D55D02010000005900000074030000080011000000000000001200001D000000000000012000A04500000000060373746404FF00FF00FF0012FF00746573745F62696E6C6F675F72656164657200424547494E4817F0A8",
            //TableMapEvent
            "00D7D7D55D13010000004E000000C203000000005A0000000000010012746573745F62696E6C6F675F726561646572000A746573745F7461626C65000203FE02FE50000101800203FCFF00FFD465D9",
            //WriteRowsEventV2
            "00D7D7D55D1E010000002E000000F003000000005A00000000000100020002FF00010000000574657374317B298B38",
            //XidEvent
            "00D7D7D55D10010000001F0000000F0400000000560000000000000026E09DF6",
            //AnonymousGtidEvent
            "000CD8D55D22010000004F0000005E04000000000000000000000000000000000000000000000000000000000002030000000000000004000000000000005B3DE13FD09705FC5901923801005113304E",
            //QueryEvent
            "000CD8D55D020100000062000000C00400000800110000000000000012000026000000000000012000A04500000000060373746404FF00FF00FF0009010000000000000012FF00746573745F62696E6C6F675F72656164657200424547494E155383C7",
            //TableMapEvent
            "000CD8D55D13010000004E0000000E05000000005A0000000000010012746573745F62696E6C6F675F726561646572000A746573745F7461626C65000203FE02FE50000101800203FCFF00DC074387",
            //UpdateRowsEvent
            "000CD8D55D1F010000003B0000004905000000005A00000000000100020002FFFF0001000000057465737431000100000006746573742D316F0ADDE1",
            //XidEvent
            "000CD8D55D10010000001F00000068050000000057000000000000002CE1AC90",
    };
}