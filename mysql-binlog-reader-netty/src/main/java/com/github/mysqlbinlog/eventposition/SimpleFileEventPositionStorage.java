package com.github.mysqlbinlog.eventposition;

import com.github.mysqlbinlogreader.common.eventposition.EventPosition;
import com.github.mysqlbinlogreader.common.eventposition.EventPositionStorage;

import java.nio.file.Files;
import java.nio.file.Paths;

public class SimpleFileEventPositionStorage implements EventPositionStorage {
    private String fileName;


    public SimpleFileEventPositionStorage() {
    }

    @Override
    public void saveCurrent(EventPosition eventPosition) {
        try {
            String position = eventPosition.getBinlogFileName() + ":" + eventPosition.getPosition();
            Files.write(Paths.get(fileName), position.getBytes());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    @Override
    public EventPosition getCurrent() {
        try {
            String position = new String(Files.readAllBytes(Paths.get(fileName)));
            String [] parts = position.split(":");
            if (parts.length == 2) {
                return new EventPosition(parts[0], Long.parseLong(parts[1]));
            } else {
                throw new RuntimeException("Unable to parse current position [" + position + "]");
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
