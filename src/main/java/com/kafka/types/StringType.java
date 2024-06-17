package com.kafka.types;

public class StringType extends DataType {
    StringType() {
    }
    @Override
    public String simpleString() {
        return "string";
    }
}
