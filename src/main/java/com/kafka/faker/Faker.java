package com.kafka.faker;

import java.io.Serializable;

public abstract class Faker<T> implements Serializable {
    public abstract T geneValue() throws Exception;

    public void init(int instanceCount, int instanceIndex) throws Exception {}
    public void destroy() throws Exception {}

    public boolean isUnionFaker(){
        return false;
    }
}
