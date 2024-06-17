package com.kafka.faker;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ObjectFaker extends Faker<Map<String, Object>> {
    private final FieldFaker[] fields;
    private final int initialCapacity;

    public ObjectFaker(FieldFaker[] fields) {
        this.fields = fields;
        this.initialCapacity = (int) (fields.length / 0.75);
    }

    @Override
    public void init(int instanceCount, int instanceIndex) throws Exception {
        for (FieldFaker field : fields) {
            field.faker.init(instanceCount, instanceIndex);
        }
    }

    @Override
    public Map<String, Object> geneValue() throws Exception {
        Map<String, Object> map = new LinkedHashMap<>(initialCapacity);
        FieldFaker fieldFaker;
        Object value;
        for (int i = 0; i < fields.length; i++) {
            fieldFaker = fields[i];
            value = fieldFaker.faker.geneValue();
            if(value == null){
                continue;
            }
            if (!fieldFaker.faker.isUnionFaker()) {
                map.put(fieldFaker.name, value);
            } else {
                for (UnionFaker.FieldValue fieldValue : (List<UnionFaker.FieldValue>) value) {
                    map.put(fieldValue.name, fieldValue.value);
                }
            }
        }
        return map;
    }

    @Override
    public void destroy() throws Exception {
        for (FieldFaker field : fields) {
            field.faker.destroy();
        }
    }

    public static final class FieldFaker implements Serializable {
        public final String name;
        public final Faker<?> faker;

        public FieldFaker(String name, Faker<?> faker) {
            this.name = name;
            this.faker = faker;
        }
    }
}
