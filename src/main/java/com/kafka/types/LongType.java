package com.kafka.types;

public class LongType extends DataType {
    LongType() {
    }
    @Override
    public String simpleString() {
        return "bigint";
    }
}
