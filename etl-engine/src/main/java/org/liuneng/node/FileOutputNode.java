package org.liuneng.node;

import org.liuneng.base.Dataflow;
import org.liuneng.base.Node;
import org.liuneng.base.OutputNode;
import org.liuneng.base.Row;
import org.liuneng.exception.NodePrestartException;
import org.liuneng.exception.NodeWritingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FileOutputNode extends Node implements OutputNode {
    final static Logger log = LoggerFactory.getLogger(FileOutputNode.class);

    private final String filePath;

    private FileOutputStream fileOutputStream;

    private String format;

    private boolean firstWrite = true;

    public FileOutputNode(String filePath, String format) {
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
            if ("JSON".equalsIgnoreCase(format)) {
                if (firstWrite) {
                    fileOutputStream.write('[');
                }

                if (row.isEnd()) {
                    fileOutputStream.write(']');
                    return;
                }

                fileOutputStream.write((row.toJSONString()+",\n").getBytes(StandardCharsets.UTF_8));
            } else {
                fileOutputStream.write((row.toString()+"\n").getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new NodeWritingException(e.getMessage());
        }

        if (firstWrite) {
            firstWrite = false;
        }
    }

    @Override
    public String[] getOutputColumns() throws Exception {
        return new String[0];
    }


    @Override
    protected void prestart(Dataflow dataflow) throws NodePrestartException {
        super.prestart(dataflow);
        File file = new File(filePath);
        try {
            fileOutputStream = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            throw new NodePrestartException(e.getMessage());
        }
    }

    @Override
    protected void onDataflowStop() {
        try {
            fileOutputStream.close();
        } catch (IOException e) {
            log.error("关闭异常", e);
        }

    }
}
