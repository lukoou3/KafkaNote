package com.kafka.faker;

public class UniqueSequenceFaker extends Faker<Long> {
    private final long start;
    private long n;
    private long k;
    private long value;

    public UniqueSequenceFaker(long start) {
        this.start = start;
    }

    @Override
    public void init(int instanceCount, int instanceIndex) throws Exception {
        super.init(instanceCount, instanceIndex);
        n = instanceCount;
        k = instanceIndex;
        value = start + k;
    }

    @Override
    public Long geneValue() throws Exception {
        Long rst = value;
        value += n;
        return rst;
    }

}
