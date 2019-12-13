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

package com.github.mysqlbinlogreader.common;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mysql.io.MysqlBinlogByteArrayInputStream;
import com.github.mysql.protocol.deserializer.ErrorResponsePacketDeserializer;
import com.github.mysql.protocol.deserializer.GreetingResponsePacketDeserializer;
import com.github.mysql.protocol.deserializer.ResultSetFieldResponsePacketDeserializer;
import com.github.mysql.protocol.deserializer.ResultSetHeaderResponsePacketDeserializer;
import com.github.mysql.protocol.deserializer.ResultSetRowResponsePacketDeserializer;
import com.github.mysql.protocol.model.BinlogDumpCmdPacket;
import com.github.mysql.protocol.model.ErrorResponsePacket;
import com.github.mysql.protocol.model.QueryCmdPacket;
import com.github.mysql.protocol.model.RawMysqlPacket;
import com.github.mysql.protocol.model.ResultSetFieldResponsePacket;
import com.github.mysql.protocol.model.ResultSetHeaderResponsePacket;
import com.github.mysql.protocol.model.ResultSetRowResponsePacket;
import com.github.mysqlbinlog.event.checksum.MysqlChecksumFactory;
import com.github.mysqlbinlog.event.deserializer.BinlogDeserializerContext;
import com.github.mysqlbinlog.event.deserializer.BinlogEventDeserializerFactory;
import com.github.mysqlbinlog.event.deserializer.BinlogEventFactory;
import com.github.mysqlbinlog.event.deserializer.BinlogEventHeaderDeserializer;
import com.github.mysqlbinlog.event.deserializer.SimpleBinlogDeserializerContextImpl;
import com.github.mysqlbinlog.event.deserializer.SimpleBinlogEventDeserializerFactoryImpl;
import com.github.mysqlbinlog.event.deserializer.SimpleBinlogEventFactoryImpl;
import com.github.mysqlbinlog.event.deserializer.SimpleBinlogEventHeaderDeserializerImpl;
import com.github.mysqlbinlog.event.deserializer.SimpleSingleBinglogEventDeserializerImpl;
import com.github.mysqlbinlog.event.deserializer.SingleBinglogEventDeserializer;
import com.github.mysqlbinlog.model.event.BinlogEvent;
import com.github.mysqlbinlog.model.event.extra.ColumnExtraData;
import com.github.mysqlbinlogreader.common.eventposition.EventPosition;
import com.github.mysqlbinlogreader.common.exception.RuntimeMysqlBinlogClientException;
import com.github.mysqlbinlogreader.common.exception.RuntimeMysqlErrorException;

public abstract class AbstractMysqlBinlogReaderImpl implements MysqlBinlogReader {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMysqlBinlogReaderImpl.class);

    private Connection connection;

    private GreetingResponsePacketDeserializer greetingResponsePacketDeserializer;
    private ErrorResponsePacketDeserializer errorResponsePacketDeserializer;

    private ResultSetHeaderResponsePacketDeserializer resultSetHeaderResponsePacketDeserializer;
    private ResultSetFieldResponsePacketDeserializer resultSetFieldResponsePacketDeserializer;
    private ResultSetRowResponsePacketDeserializer resultSetRowResponsePacketDeserializer;

    private SingleBinglogEventDeserializer singleBinglogEventDeserializer;

    private SimpleBinlogDeserializerContextImpl binlogDeserializerContext;


    private Map<String, String> variables;

    private int serverId;

    private EventPosition eventPosition;

    protected abstract Connection initializeConnection();

    public void initialize() {

        if (this.variables == null) {
            this.variables = new HashMap<>();
        }

        if (this.binlogDeserializerContext == null) {
            this.binlogDeserializerContext = new SimpleBinlogDeserializerContextImpl();
        }

        if (this.singleBinglogEventDeserializer == null) {
            BinlogEventHeaderDeserializer binlogEventHeaderParser = new SimpleBinlogEventHeaderDeserializerImpl();
            BinlogEventFactory binlogEventFactory = new SimpleBinlogEventFactoryImpl();
            BinlogEventDeserializerFactory binlogEventDeserializerFactory = new SimpleBinlogEventDeserializerFactoryImpl();

            SimpleSingleBinglogEventDeserializerImpl simpleSingleBinglogEventDeserializer = new SimpleSingleBinglogEventDeserializerImpl();
            simpleSingleBinglogEventDeserializer.setBinlogEventHeaderParser(binlogEventHeaderParser);
            simpleSingleBinglogEventDeserializer.setBinlogEventFactory(binlogEventFactory);
            simpleSingleBinglogEventDeserializer.setBinlogEventUnmarshallerFactory(binlogEventDeserializerFactory);

            this.singleBinglogEventDeserializer = simpleSingleBinglogEventDeserializer;
        }

        if (this.greetingResponsePacketDeserializer == null) {
            this.greetingResponsePacketDeserializer = new GreetingResponsePacketDeserializer();
        }

        if (this.errorResponsePacketDeserializer == null) {
            this.errorResponsePacketDeserializer = new ErrorResponsePacketDeserializer();
        }

        if (this.resultSetHeaderResponsePacketDeserializer == null) {
            this.resultSetHeaderResponsePacketDeserializer = new ResultSetHeaderResponsePacketDeserializer();
        }

        if (this.resultSetFieldResponsePacketDeserializer == null) {
            this.resultSetFieldResponsePacketDeserializer = new ResultSetFieldResponsePacketDeserializer();
        }

        if (this.resultSetRowResponsePacketDeserializer == null) {
            this.resultSetRowResponsePacketDeserializer = new ResultSetRowResponsePacketDeserializer();
        }

        if (this.connection == null) {
            this.connection = this.initializeConnection();
        }
    }


    public void connect() {
        this.connection.connect();
    }

    public void disconnect() {
        this.connection.close();
    }

    private void checkQueryResponsePacket(RawMysqlPacket packet) {
        if (packet == null) {
            throw new RuntimeMysqlBinlogClientException("ERROR [ packet is null ]");
        }

        if (packet.isErrorPacket()) {
            ErrorResponsePacket errorResponsePacket = 
                    (ErrorResponsePacket) errorResponsePacketDeserializer.deserialize(
                            new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
            onMysqlError(errorResponsePacket);
        }

        if (packet.isEOFPacket()) {
            throw new RuntimeMysqlBinlogClientException("EOF ERROR [ Unknown ]");
        }
    }
    
    private RawMysqlPacket skipEofOkPackets() {
        RawMysqlPacket packet = null;
        do {
            packet = connection.readRawPacket();

            if (logger.isDebugEnabled()) {
                logger.debug("receiveSettings3 response [{}]", packet);
            }
        }
        while(packet.isEOFPacket() || packet.isOKPacket());
        
       return packet;
    }
    
    public void receiveSettings() {
        QueryCmdPacket cmd = new QueryCmdPacket("SHOW GLOBAL VARIABLES");
        connection.writeRawPacket(cmd);

        RawMysqlPacket packet = connection.readRawPacket();

        if (logger.isDebugEnabled()) {
            logger.debug("receiveSettings1 response [{}]", packet);
        }

        checkQueryResponsePacket(packet);
        
        ResultSetHeaderResponsePacket resultSetHeaderResponsePacket = 
                (ResultSetHeaderResponsePacket)resultSetHeaderResponsePacketDeserializer.deserialize(
                        new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody()))); 


        for (int i = 0; i < resultSetHeaderResponsePacket.getFieldCount(); i++) {
            packet = connection.readRawPacket();

            if (logger.isDebugEnabled()) {
                logger.debug("receiveSettings2 response [{}]", packet);
            }

            if (packet.isErrorPacket()) {
                ErrorResponsePacket errorResponsePacket = 
                        (ErrorResponsePacket) errorResponsePacketDeserializer.deserialize(
                                new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
                onMysqlError(errorResponsePacket);
            }

            /*
             * in case we need to get column definitions 
             * ResultSetFieldResponsePacket resultSetFieldResponsePacket = 
             *        (ResultSetFieldResponsePacket)resultSetFieldResponsePacketDeserializer.deserialize(
             *                new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())))
             */

        }

        packet = skipEofOkPackets();

        while (!(packet.isEOFPacket() || packet.isOKPacket())) {
            if (packet.isErrorPacket()) {
                ErrorResponsePacket errorResponsePacket = 
                        (ErrorResponsePacket) errorResponsePacketDeserializer.deserialize(
                                new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
                onMysqlError(errorResponsePacket);
            }

            ResultSetRowResponsePacket resultSetRowResponsePacket = 
                    (ResultSetRowResponsePacket)resultSetRowResponsePacketDeserializer.deserialize(
                            new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));

            variables.put(resultSetRowResponsePacket.getValues().get(0), resultSetRowResponsePacket.getValues().get(1));

            packet = connection.readRawPacket();

            if (logger.isDebugEnabled()) {
                logger.debug("receiveSettings4 response [{}]", packet);
            }

        }
    }

    public void bindSettings() {
        String checksumType = this.variables.get("binlog_checksum");
        binlogDeserializerContext.setChecksum(MysqlChecksumFactory.create(checksumType));

        if (checksumType != null && checksumType.length() > 0 && !checksumType.equalsIgnoreCase("NONE")) {
            QueryCmdPacket cmd = new QueryCmdPacket("SET @master_binlog_checksum= '@@global.binlog_checksum'");
            connection.writeRawPacket(cmd);

            RawMysqlPacket packet = connection.readRawPacket();

            if (logger.isDebugEnabled()) {
                logger.debug("bindSettings1 response [{}]", packet);
            }

            if (packet.isErrorPacket()) {
                ErrorResponsePacket errorResponsePacket = 
                        (ErrorResponsePacket) errorResponsePacketDeserializer.deserialize(
                                new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
                onMysqlError(errorResponsePacket);
            }

            if (packet.isEOFPacket()) {
                throw new RuntimeMysqlBinlogClientException("bindSettings ERROR [ Unknown ]");
            }
        }
    }

    public void readMetaData() {
        QueryCmdPacket cmd = new QueryCmdPacket(
                "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE " 
                        + "FROM information_schema.COLUMNS " 
                        + "ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION");
        connection.writeRawPacket(cmd);

        RawMysqlPacket packet = connection.readRawPacket();

        checkQueryResponsePacket(packet);
        
        ResultSetHeaderResponsePacket resultSetHeaderResponsePacket = 
                (ResultSetHeaderResponsePacket)resultSetHeaderResponsePacketDeserializer.deserialize(
                        new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));

        if (logger.isDebugEnabled()) {
            logger.debug("readMetaData1 response [{}]\n[{}]", packet, resultSetHeaderResponsePacket);
        }


        for (int i = 0; i < resultSetHeaderResponsePacket.getFieldCount(); i++) {
            packet = connection.readRawPacket();

            if (packet.isErrorPacket()) {
                ErrorResponsePacket errorResponsePacket = 
                        (ErrorResponsePacket) errorResponsePacketDeserializer.deserialize(
                                new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
                onMysqlError(errorResponsePacket);
            }

            /* in case column definitions are needed */
            ResultSetFieldResponsePacket resultSetFieldResponsePacket = 
                    (ResultSetFieldResponsePacket)resultSetFieldResponsePacketDeserializer.deserialize(
                            new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));

            if (logger.isDebugEnabled()) {
                logger.debug("readMetaData2 response [{}]\n[{}]\n", packet, resultSetFieldResponsePacket);
            }

        }

        packet = skipEofOkPackets();

        while (!(packet.isEOFPacket() || packet.isOKPacket())) {
            if (packet.isErrorPacket()) {
                ErrorResponsePacket errorResponsePacket = 
                        (ErrorResponsePacket) errorResponsePacketDeserializer.deserialize(
                                new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
                onMysqlError(errorResponsePacket);
            }

            ResultSetRowResponsePacket resultSetRowResponsePacket = 
                    (ResultSetRowResponsePacket)resultSetRowResponsePacketDeserializer.deserialize(
                            new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
            packet = connection.readRawPacket();

            if (logger.isDebugEnabled()) {
                logger.debug("readMetaData4 response [{}]\n[{}]", packet, resultSetRowResponsePacket);
            }


            String databaseName = resultSetRowResponsePacket.getValues().get(0);
            String tableName = resultSetRowResponsePacket.getValues().get(1);
            String columnName = resultSetRowResponsePacket.getValues().get(2);
            String columnType = resultSetRowResponsePacket.getValues().get(3);

            List<ColumnExtraData> columnsExtraItems = binlogDeserializerContext.getColumnExtra(databaseName, tableName);
            if (columnsExtraItems == null) {
                columnsExtraItems = new ArrayList<>();
                binlogDeserializerContext.addColumnExtra(databaseName, tableName, columnsExtraItems);
            }

            columnsExtraItems.add(new ColumnExtraData(columnName, columnType));
        }

    }

    public void readCurrentBinlogPosition() {
        QueryCmdPacket cmd = new QueryCmdPacket("SHOW MASTER STATUS");
        connection.writeRawPacket(cmd);

        RawMysqlPacket packet = connection.readRawPacket();

        if (logger.isDebugEnabled()) {
            logger.debug("readCurrentBinlogPosition1 packet [{}]", packet);
        }

        checkQueryResponsePacket(packet);
        
        ResultSetHeaderResponsePacket resultSetHeaderResponsePacket = 
                (ResultSetHeaderResponsePacket)resultSetHeaderResponsePacketDeserializer.deserialize(
                        new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody()))); 

        for (int i = 0; i < resultSetHeaderResponsePacket.getFieldCount(); i++) {
            packet = connection.readRawPacket();

            if (logger.isDebugEnabled()) {
                logger.debug("readCurrentBinlogPosition2 packet [{}]", packet);
            }

            if (packet.isErrorPacket()) {
                ErrorResponsePacket errorResponsePacket = 
                        (ErrorResponsePacket) errorResponsePacketDeserializer.deserialize(
                                new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
                onMysqlError(errorResponsePacket);
            }

            /*
             * in case column definitions are needed
             *       ResultSetFieldResponsePacket resultSetFieldResponsePacket = 
             *       (ResultSetFieldResponsePacket)resultSetFieldResponsePacketDeserializer.deserialize(
             *               new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())))
             */
        }

        packet = skipEofOkPackets();

        while (!(packet.isEOFPacket() || packet.isOKPacket())) {
            if (packet.isErrorPacket()) {
                ErrorResponsePacket errorResponsePacket = 
                        (ErrorResponsePacket) errorResponsePacketDeserializer.deserialize(
                                new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
                onMysqlError(errorResponsePacket);
            }

            ResultSetRowResponsePacket resultSetRowResponsePacket = 
                    (ResultSetRowResponsePacket)resultSetRowResponsePacketDeserializer.deserialize(
                            new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
            packet = connection.readRawPacket();

            if (logger.isDebugEnabled()) {
                logger.debug("readCurrentBinlogPosition3 ResultSetRowResponsePacket [{}]", resultSetRowResponsePacket);
            }

            if (this.eventPosition == null) {
                this.eventPosition = new EventPosition(resultSetRowResponsePacket.getValues().get(0), 
                        Long.parseLong(resultSetRowResponsePacket.getValues().get(1)));
            }
        }

    }



    public void dumpBinglog() {
        BinlogDumpCmdPacket cmd = new BinlogDumpCmdPacket(this.serverId, 0x00, 
                this.eventPosition.getPosition(), this.eventPosition.getBinlogFileName());
        connection.writeRawPacket(cmd);

        RawMysqlPacket packet = connection.readRawPacket();

        if (logger.isDebugEnabled()) {
            logger.debug("dumpBinglog1 response [{}][{}][{}]", packet, this.eventPosition.getBinlogFileName(), this.eventPosition.getPosition());
        }

        if (packet.isErrorPacket()) {
            ErrorResponsePacket errorResponsePacket = 
                    (ErrorResponsePacket) errorResponsePacketDeserializer.deserialize(
                            new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
            onMysqlError(errorResponsePacket);
        }

        if (packet.isEOFPacket()) {
            throw new RuntimeMysqlBinlogClientException("dumpBinglog ERROR [ Unknown ]");
        }
    }

    public void onMysqlError(ErrorResponsePacket errorResponsePacket) {
        throw new RuntimeMysqlErrorException(errorResponsePacket);
    }


    /* (non-Javadoc)
     * @see com.github.mysqlbinlogreader.common.MysqlBinlogReader#open()
     */
    public void open() {
        initialize();

        connect();

        receiveSettings();

        bindSettings();

        readMetaData();

        if (this.eventPosition == null) {
            readCurrentBinlogPosition();
        }

        dumpBinglog();

    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlogreader.common.MysqlBinlogReader#readBinlogEvent()
     */
    public BinlogEvent readBinlogEvent() {
        RawMysqlPacket packet = connection.readRawPacket();

        if (logger.isDebugEnabled()) {
            logger.debug("readBinlogEvent1 response [{}]", packet);
        }

        if (packet.isErrorPacket()) {
            ErrorResponsePacket errorResponsePacket = 
                    (ErrorResponsePacket) errorResponsePacketDeserializer.deserialize(
                            new MysqlBinlogByteArrayInputStream(new ByteArrayInputStream(packet.getRawBody())));
            throw new RuntimeMysqlErrorException(errorResponsePacket);
        }

        if (packet.isEOFPacket()) {
            throw new RuntimeMysqlBinlogClientException("EOF packet");
        }

        if (!packet.isOKPacket()) {
            throw new RuntimeMysqlBinlogClientException("Invalid binlog event [" + packet + "]");
        }


        return singleBinglogEventDeserializer.deserialize(packet.getRawBody(), this.binlogDeserializerContext);
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlogreader.common.MysqlBinlogReader#close()
     */
    public void close() {
        disconnect();
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlogreader.common.MysqlBinlogReader#getBinlogDeserializerContext()
     */
    public BinlogDeserializerContext getBinlogDeserializerContext() {
        return this.binlogDeserializerContext;
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlogreader.common.MysqlBinlogReader#setEventPosition(com.github.mysqlbinlogreader.common.eventposition.EventPosition)
     */
    public void setEventPosition(EventPosition eventPosition) {
        this.eventPosition = eventPosition;
    }

    /* (non-Javadoc)
     * @see com.github.mysqlbinlogreader.common.MysqlBinlogReader#getEventPosition()
     */
    public EventPosition getEventPosition() {
        return this.eventPosition;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }


    public GreetingResponsePacketDeserializer getGreetingResponsePacketDeserializer() {
        return greetingResponsePacketDeserializer;
    }


    public void setGreetingResponsePacketDeserializer(
            GreetingResponsePacketDeserializer greetingResponsePacketDeserializer) {
        this.greetingResponsePacketDeserializer = greetingResponsePacketDeserializer;
    }


    public ErrorResponsePacketDeserializer getErrorResponsePacketDeserializer() {
        return errorResponsePacketDeserializer;
    }


    public void setErrorResponsePacketDeserializer(
            ErrorResponsePacketDeserializer errorResponsePacketDeserializer) {
        this.errorResponsePacketDeserializer = errorResponsePacketDeserializer;
    }


    public ResultSetHeaderResponsePacketDeserializer getResultSetHeaderResponsePacketDeserializer() {
        return resultSetHeaderResponsePacketDeserializer;
    }


    public void setResultSetHeaderResponsePacketDeserializer(
            ResultSetHeaderResponsePacketDeserializer resultSetHeaderResponsePacketDeserializer) {
        this.resultSetHeaderResponsePacketDeserializer = resultSetHeaderResponsePacketDeserializer;
    }


    public ResultSetFieldResponsePacketDeserializer getResultSetFieldResponsePacketDeserializer() {
        return resultSetFieldResponsePacketDeserializer;
    }


    public void setResultSetFieldResponsePacketDeserializer(
            ResultSetFieldResponsePacketDeserializer resultSetFieldResponsePacketDeserializer) {
        this.resultSetFieldResponsePacketDeserializer = resultSetFieldResponsePacketDeserializer;
    }


    public ResultSetRowResponsePacketDeserializer getResultSetRowResponsePacketDeserializer() {
        return resultSetRowResponsePacketDeserializer;
    }


    public void setResultSetRowResponsePacketDeserializer(
            ResultSetRowResponsePacketDeserializer resultSetRowResponsePacketDeserializer) {
        this.resultSetRowResponsePacketDeserializer = resultSetRowResponsePacketDeserializer;
    }


    public SingleBinglogEventDeserializer getSingleBinglogEventDeserializer() {
        return singleBinglogEventDeserializer;
    }


    public void setSingleBinglogEventDeserializer(
            SingleBinglogEventDeserializer singleBinglogEventDeserializer) {
        this.singleBinglogEventDeserializer = singleBinglogEventDeserializer;
    }


    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }


    public Map<String, String> getVariables() {
        return variables;
    }


    public void setBinlogDeserializerContext(
            SimpleBinlogDeserializerContextImpl binlogDeserializerContext) {
        this.binlogDeserializerContext = binlogDeserializerContext;
    }


}
