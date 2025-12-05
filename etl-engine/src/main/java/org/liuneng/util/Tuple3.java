package org.liuneng.util;

public class Tuple3<T1, T2, T3> {

    private T1 partA;
    private T2 partB;
    private T3 partC;

    public Tuple3(T1 partA, T2 partB, T3 partC) {
        this.partA = partA;
        this.partB = partB;
        this.partC = partC;
    }

    public T1 getPartA() {
        return partA;
    }

    public T2 getPartB() {
        return partB;
    }

    public void setPartA(T1 partA) {
        this.partA = partA;
    }

    public void setPartB(T2 partB) {
        this.partB = partB;
    }

    public T3 getPartC() {
        return partC;
    }

    public void setPartC(T3 partC) {
        this.partC = partC;
    }
}
