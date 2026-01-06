package org.liuneng.base;

//import cn.hutool.core.map.CaseInsensitiveLinkedMap;
import cn.hutool.json.JSONUtil;
import lombok.Getter;
import lombok.Setter;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.util.HashMap;
import java.util.Map;

public class Row {

    @Setter @Getter
    private boolean end = false;

    @Setter @Getter
    private boolean empty = false;

    @Setter @Getter
    private int pipeIndex = 0;

    private final Map<String, Object> data;

    private Row(Map<String, Object> data) {
        this.data = data;
    }

    public static Row ofEnd() {
        Row row = new Row(new HashMap<>());
        row.setEnd(true);
        return row;
    }

    public static Row ofEmpty() {
        Row row = new Row(new HashMap<>());
        row.setEmpty(true);
        return row;
    }

    public static Row ofMap(Map<String, Object> data) {
        Row row = new Row(data);
        return row;
    }

    public static Row ofMap(Map<String, Object> data, int pipeIndex) {
        Row row = new Row(data);
        row.setPipeIndex(pipeIndex);
        return row;
    }

    public Map<String, Object> getMap() {
        return data;
    }

    public Object get(String key) {
        return data.get(key);
    }

    public void put(String key, Object val) {
        this.data.put(key, val);
    }

    @Override
    public String toString() {
        return data.toString();
    }

    public String toJSONString() {
        return JSONUtil.toJsonStr(data);
    }

}
