package org.liuneng.base;

public interface MiddleNode_new {

    enum Type {
        COPY, SWITCH
    }

    Row process(Row row);

    default Type getType() {
        return Type.COPY;
    }

    default Node asNode() {
        return (Node) this;
    }
}
