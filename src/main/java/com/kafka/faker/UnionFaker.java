package com.kafka.faker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class UnionFaker extends Faker<List<UnionFaker.FieldValue>> {
    private final FieldsFaker[] fieldsFakers;
    private final boolean random;
    private final FieldValue[] fieldValuesTmp;
    private final List<FieldValue> fieldValues;
    private final int[] weights;
    private final int weightMax;
    private Random r;
    private int index = 0;

    public UnionFaker(FieldsFaker[] fieldsFakers, boolean random) {
        this.fieldsFakers = fieldsFakers;
        this.random = random;
        this.fieldValuesTmp = new FieldValue[Arrays.stream(fieldsFakers).mapToInt(x -> x.fields.length).max().getAsInt()];
        this.fieldValues = new ArrayList<>();
        this.weights = new int[fieldsFakers.length];
        weights[0] = fieldsFakers[0].weight;
        for (int i = 1; i < fieldsFakers.length; i++) {
            weights[i] = fieldsFakers[i].weight + weights[i - 1];
        }
        this.weightMax = weights[weights.length - 1];
        for (int i = 0; i < fieldValuesTmp.length; i++) {
            fieldValuesTmp[i] = new FieldValue();
        }
    }

    @Override
    public void init(int instanceCount, int instanceIndex) throws Exception {
        r = new Random();
        for (FieldsFaker fieldsFaker : fieldsFakers) {
            for (ObjectFaker.FieldFaker field : fieldsFaker.fields) {
                field.faker.init(instanceCount, instanceIndex);
            }
        }
    }

    @Override
    public List<FieldValue> geneValue() throws Exception {
        fieldValues.clear();

        FieldsFaker fieldsFaker;
        ObjectFaker.FieldFaker[] fakers;
        ObjectFaker.FieldFaker fieldFaker;
        Object value;
        FieldValue fieldValue;

        if (!random) {
            fieldsFaker = fieldsFakers[index];
            if (fieldsFaker.weightIndex == fieldsFaker.weight) {
                fieldsFaker.weightIndex = 0;
                index++;
                if (index == fieldsFakers.length) {
                    index = 0;
                }
                fieldsFaker = fieldsFakers[index];
            }
            fieldsFaker.weightIndex++;
        } else {
            int key = r.nextInt(weightMax) + 1;
            int index = Arrays.binarySearch(weights, key);
            if (index < 0) {
                index = -index - 1;
            }
            fieldsFaker = fieldsFakers[index];
        }

        fakers = fieldsFaker.fields;
        for (int i = 0; i < fakers.length; i++) {
            fieldFaker = fakers[i];
            value = fieldFaker.faker.geneValue();
            if (value != null) {
                fieldValue = fieldValuesTmp[i];
                fieldValue.name = fieldFaker.name;
                fieldValue.value = value;
                fieldValues.add(fieldValue);
            }
        }

        return fieldValues;
    }

    @Override
    public void destroy() throws Exception {
        for (FieldsFaker fieldsFaker : fieldsFakers) {
            for (ObjectFaker.FieldFaker field : fieldsFaker.fields) {
                field.faker.destroy();
            }
        }
    }

    @Override
    public boolean isUnionFaker() {
        return true;
    }

    public static class FieldValue implements Serializable {
        public String name;
        public Object value;
    }

    public static class FieldsFaker implements Serializable {
        final ObjectFaker.FieldFaker[] fields;
        final int weight;
        int weightIndex = 0;

        public FieldsFaker(ObjectFaker.FieldFaker[] fields, int weight) {
            this.fields = fields;
            this.weight = weight;
        }
    }

}
