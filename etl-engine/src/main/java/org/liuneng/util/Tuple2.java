package org.liuneng.util;

public class Tuple2<T1, T2> {

    private T1 partA;
    private T2 partB;

    public Tuple2(T1 partA, T2 partB) {
        this.partA = partA;
        this.partB = partB;
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

    @Override
    public String toString() {
        return String.format("(%s, %s)", partA, partB);
    }
}
