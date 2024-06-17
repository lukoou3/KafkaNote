package com.kafka.faker;

public class SequenceFaker extends Faker<Long> {
    private final long start;
    private final long step;
    private long value;

    public SequenceFaker(long start) {
        this(start, 1);
    }

    public SequenceFaker(long start, long step) {
        this.start = start;
        this.step = step;
        this.value = start;
    }

    @Override
    public Long geneValue() throws Exception {
        Long rst = value;
        value += step;
        return rst;
    }

}
