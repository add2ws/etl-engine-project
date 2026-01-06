package org.liuneng.nodeextension;


public enum UpsertTag {

    COMPARE_ONLY(1), //仅比对
    UPDATE_ONLY(2), //仅更新
    COMPARE_AND_UPDATE(3); //比对且更新

    private final int code;

    UpsertTag(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static UpsertTag fromCode(int code) {
        for (UpsertTag upsertTag : UpsertTag.values()) {
            if (upsertTag.getCode() == code) {
                return upsertTag;
            }
        }
        return null;
    }
}
