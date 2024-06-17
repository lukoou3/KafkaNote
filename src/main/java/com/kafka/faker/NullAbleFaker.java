package com.kafka.faker;

import java.util.concurrent.ThreadLocalRandom;

public class NullAbleFaker<T> extends Faker<T> {

    private Faker<T> faker;
    private double nullRate;

    private NullAbleFaker(Faker<T> faker, double nullRate) {
        this.faker = faker;
        this.nullRate = nullRate;
    }

    public static <T> Faker<T> wrap(Faker<T> faker, double nullRate) {
        if (nullRate > 0D) {
            return new NullAbleFaker<>(faker, nullRate);
        } else {
            return faker;
        }
    }

    @Override
    public void init(int instanceCount, int instanceIndex) throws Exception {
        faker.init(instanceCount, instanceIndex);
    }

    @Override
    public T geneValue() throws Exception {
        if (ThreadLocalRandom.current().nextDouble() < nullRate) {
            return null;
        } else {
            return faker.geneValue();
        }
    }

    @Override
    public void destroy() throws Exception {
        faker.destroy();
    }
}
