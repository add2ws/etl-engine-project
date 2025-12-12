package org.liuneng.util;

import java.util.Collection;
import java.util.stream.Collectors;

public class CsvConverter {

    public static String ListToCsvRow(Collection<Object> valueList) {
        if (valueList == null) {
            return "";
        }

        String csvRow = valueList.stream().map(CsvConverter::formatCsvValue).collect(Collectors.joining(","));
        return csvRow;
    }

    /**
     * 将任何对象转换为适合 CSV 格式的字符串，并处理特殊字符。
     * 遵循标准的 CSV 引用和转义规则：
     * - 如果值包含逗号、双引号或换行符，则整个值用双引号包裹。
     * - 值中的双引号需要转义为两个双引号 (""")。
     *
     * @param value 待格式化的对象
     * @return 格式化后的 CSV 字段字符串
     */
    private static String formatCsvValue(Object value) {
        if (value == null) {
            return "";
        }

        // 将值转换为字符串
        String stringValue = value.toString();

        // 检查是否需要引用（即是否包含特殊字符：逗号, 双引号, 换行符）
        if (stringValue.contains(",") || stringValue.contains("\"") || stringValue.contains("\n")) {

            // 1. 转义内部的双引号：将 " 替换为 ""
            String escapedValue = stringValue.replace("\"", "\"\"");

            // 2. 用双引号将整个值包裹起来
            return "\"" + escapedValue + "\"";
        }

        // 如果不包含特殊字符，则直接返回原字符串
        return stringValue;
    }
}