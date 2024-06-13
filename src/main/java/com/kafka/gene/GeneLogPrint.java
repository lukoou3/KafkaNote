package com.kafka.gene;

import com.alibaba.fastjson2.JSON;
import com.utils.ArgsTool;

import java.util.HashMap;
import java.util.List;

public class GeneLogPrint {

    // --gene "[{\"type\":\"int_random\", \"fields\":{\"name\":\"id\"}}, {\"type\":\"str_fix\", \"fields\":{\"name\":\"name\",\"len\":5}}]"
    public static void main(String[] args)  throws Exception{
        ArgsTool argsTool = ArgsTool.fromArgs(args);
        System.out.println(argsTool.getKwargs());

        List<AbstractFieldGene<?>> fieldGenes = FieldGeneUtils.createFieldGenesFromJson(argsTool.getRequired("gene"));

        for (int j = 0; j < fieldGenes.size(); j++) {
            fieldGenes.get(j).open();
        }

        AbstractFieldGene fieldGene;
        Object value;
        HashMap<String, Object> map = new HashMap<>(fieldGenes.size() * 2);

        for (int i = 0; i < 100; i++) {
            map.clear();
            for (int j = 0; j < fieldGenes.size(); j++) {
                fieldGene = fieldGenes.get(j);
                value = fieldGene.geneValue();
                if(value != null){
                    map.put(fieldGene.fieldName(), value);
                }
            }

            String ele = JSON.toJSONString(map);
            System.out.println(ele);
        }
    }

}
