package com.kafka.gene;

public abstract class AbstractFieldGene<T> {
    protected String fieldName;

    public AbstractFieldGene(String fieldName) {
        this.fieldName = fieldName;
    }

    public void open() throws Exception{

    }

    public String fieldName(){
        return fieldName;
    }

    public abstract T geneValue() throws Exception;

    public void close() throws Exception {

    }
}
