package org.liuneng.util;

import java.math.BigDecimal;
import java.util.Date;

public class StrUtil {

    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static String objectToString(Object obj) {
        return obj == null ? "" : obj.toString();
    }

    public static boolean isEqual(Object obj1, Object obj2) {
        if (obj1 instanceof BigDecimal) {
            obj1 = ((BigDecimal) obj1).stripTrailingZeros().toPlainString();
        }

        if (obj2 instanceof BigDecimal) {
            obj2 = ((BigDecimal) obj2).stripTrailingZeros().toPlainString();
        }

        if (obj1 instanceof String) {
            obj1 = ((String) obj1).trim();
        }

        if (obj2 instanceof String) {
            obj2 = ((String) obj2).trim();
        }

        if (obj1 instanceof Date) {
            obj1 = ((Date) obj1).getTime();
        }

        if (obj2 instanceof Date) {
            obj2 = ((Date) obj2).getTime();
        }

        return objectToString(obj1).equals(objectToString(obj2));
    }
}
