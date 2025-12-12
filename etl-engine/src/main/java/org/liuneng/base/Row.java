package org.liuneng.base;

//import cn.hutool.core.map.CaseInsensitiveLinkedMap;
import cn.hutool.json.JSONUtil;
import org.springframework.util.LinkedCaseInsensitiveMap;

import java.util.Map;


public class Row {

    private boolean end;

    private final Map<String, Object> data = new LinkedCaseInsensitiveMap<>();

    private Row() {}

    public static Row ofEnd() {
        Row row = new Row();
        row.setEnd(true);
        return row;
    }

    public static Row fromMap(Map<String, Object> d) {
        Row row = new Row();
        row.data.putAll(d);
        row.end = false;
        return row;
    }

    public Map<String, Object> getData() {
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

    public boolean isEnd() {
        return end;
    }

    public void setEnd(boolean end) {
        this.end = end;
    }
}
