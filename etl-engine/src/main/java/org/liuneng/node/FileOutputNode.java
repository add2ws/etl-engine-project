package org.liuneng.node;

import org.liuneng.base.Dataflow;
import org.liuneng.base.Node;
import org.liuneng.base.OutputNode;
import org.liuneng.base.Row;
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
import java.util.Collection;

public class FileOutputNode extends Node implements OutputNode {
    final static Logger log = LoggerFactory.getLogger(FileOutputNode.class);

    private final String filePath;

    private FileOutputStream fileOutputStream;

    private Format format;

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
    public long getStartTime() {
        return 0;
    }

    @Override
    public void write(Row row) throws NodeWritingException {

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
                    String csvRow = CsvConverter.ListToCsvRow(Arrays.asList(row.getData().keySet().toArray())) + "\n";
                    fileOutputStream.write(csvRow.getBytes(StandardCharsets.UTF_8));
                }

                if (row.isEnd()) {
                    fileOutputStream.write(']');
                    log.info("csv文件写入完成：{}", filePath);
                    return;
                }

                String csvRow = CsvConverter.ListToCsvRow(row.getData().values()) + "\n";
                fileOutputStream.write(csvRow.getBytes(StandardCharsets.UTF_8));

            } else {
                fileOutputStream.write((row.toString()+"\n").getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new NodeWritingException(e);
        }

        if (firstWrite) {
            firstWrite = false;
        }
    }

    @Override
    public String[] getOutputColumns() {
        return new String[0];
    }


    @Override
    protected void prestart(Dataflow dataflow) throws NodePrestartException {
        super.prestart(dataflow);
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
