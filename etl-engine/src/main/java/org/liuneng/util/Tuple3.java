package org.liuneng.util;

import lombok.Data;

@Data
public class Tuple3<T1, T2, T3> {

    private T1 partA;
    private T2 partB;
    private T3 partC;

    public Tuple3(T1 partA, T2 partB, T3 partC) {
        this.partA = partA;
        this.partB = partB;
        this.partC = partC;
    }

}
