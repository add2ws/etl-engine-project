package org.liuneng.util;

import lombok.Data;

@Data
public class Tuple2<T1, T2> {

    private T1 partA;
    private T2 partB;

    public Tuple2(T1 partA, T2 partB) {
        this.partA = partA;
        this.partB = partB;
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)", partA, partB);
    }
}
