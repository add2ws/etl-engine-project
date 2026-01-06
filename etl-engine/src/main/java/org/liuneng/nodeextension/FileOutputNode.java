package org.liuneng.nodeextension;

import lombok.NonNull;
import org.liuneng.base.*;
import org.liuneng.exception.NodePrestartException;
import org.liuneng.exception.NodeWritingException;
import org.liuneng.util.CsvConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class FileOutputNode extends Node implements OutputNode, DataProcessingMonitor {
    final static Logger log = LoggerFactory.getLogger(FileOutputNode.class);

    private final String filePath;

    private FileOutputStream fileOutputStream;

    private final Format format;

    private long processed = 0;

    public enum Format {
        JSON, CSV, TXT
    }

    private boolean firstWrite = true;



    public FileOutputNode(String filePath, Format format) {
        this.filePath = filePath;
        this.format = format;
    }


    @Override
    public long getProcessed() {
        return 0;
    }

    @Override
    public long getProcessingRate() {
        return 0;
    }

    @Override
    public long getInserted() {
        return 0;
    }

    @Override
    public long getInsertingRate() {
        return 0;
    }

    @Override
    public long getUpdated() {
        return 0;
    }

    @Override
    public long getUpdatingRate() {
        return 0;
    }

    @Override
    public long getDeleted() {
        return 0;
    }

    @Override
    public long getDeletingRate() {
        return 0;
    }

    @Override
    public long getStartTime() {
        return 0;
    }

    @Override
    public void write(@NonNull Row row) throws NodeWritingException {

        try {
            if (format == Format.JSON) {
                if (firstWrite) {
                    fileOutputStream.write('[');
                }

                if (row.isEnd()) {
                    fileOutputStream.write(']');
                    log.info("json文件写入完成：{}", filePath);
                    return;
                }

                fileOutputStream.write((row.toJSONString()+",\n").getBytes(StandardCharsets.UTF_8));

            } else if (format == Format.CSV) {
                if (firstWrite) {
                    String headRow = CsvConverter.ListToCsvRow(Arrays.asList(row.getMap().keySet().toArray())) + "\n";
                    fileOutputStream.write(headRow.getBytes(StandardCharsets.UTF_8));
                }

                if (row.isEnd()) {
                    log.info("csv文件写入完成：{}", filePath);
                    return;
                }

                String csvRow = CsvConverter.ListToCsvRow(row.getMap().values()) + "\n";
                fileOutputStream.write(csvRow.getBytes(StandardCharsets.UTF_8));

            } else {
                fileOutputStream.write((row.toString()+"\n").getBytes(StandardCharsets.UTF_8));
            }

            processed++;
            if (processed % 10000 == 0) {
                super.writeInfoLog(String.format("成功写入了%d行数据", processed));
            }

        } catch (IOException e) {
            throw new NodeWritingException(e);
        }

        if (firstWrite) {
            firstWrite = false;
        }
    }

    @Override
    protected void onDataflowPrestart() {
        File file = new File(filePath);
        try {
            fileOutputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            throw new NodePrestartException(e);
        }
    }

    @Override
    protected void onDataflowStop() {
        try {
            if (fileOutputStream != null) {
                fileOutputStream.close();
            }
        } catch (IOException e) {
            log.error("关闭异常", e);
        }

    }
}
