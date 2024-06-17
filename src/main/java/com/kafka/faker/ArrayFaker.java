package com.kafka.faker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ArrayFaker extends Faker<List<Object>> {
    private Faker<Object> faker;
    private int minLen;
    private int maxLen;
    private boolean duplicated;

    public ArrayFaker(Faker<Object> faker, int minLen, int maxLen) {
        this(faker, minLen, maxLen, true);
    }

    public ArrayFaker(Faker<Object> faker, int minLen, int maxLen, boolean duplicated) {
        Preconditions.checkArgument(!faker.isUnionFaker());
        this.faker = faker;
        this.minLen = Math.max(minLen, 0);
        this.maxLen = maxLen;
        this.duplicated = duplicated;
    }

    @Override
    public void init(int instanceCount, int instanceIndex) throws Exception {
        faker.init(instanceCount, instanceIndex);
    }

    @Override
    public List<Object> geneValue() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int size = random.nextInt(minLen, maxLen + 1);
        List<Object> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            if(duplicated){
                list.add(faker.geneValue());
            }else{
                int j = 0;
                while (j < 20){
                    Object value = faker.geneValue();
                    if(list.contains(value)){
                        j++;
                        continue;
                    }
                    list.add(value);
                    break;
                }
            }
        }
        return list;
    }

    @Override
    public void destroy() throws Exception {
        faker.destroy();
    }

}
