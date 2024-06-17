package com.kafka.faker;

import com.mifmif.common.regex.Generex;

import java.util.concurrent.ThreadLocalRandom;

public interface StringFaker {

    public static class RegexString extends Faker<String> {
        private String regex;
        private transient Generex generex;

        public RegexString(String regex) {
            Generex g = new Generex(regex);
            assert g != null;
            this.regex = regex;
        }

        @Override
        public String geneValue() throws Exception {
            if(generex == null){
                generex = new Generex(regex);
            }
            return generex.random();
        }
    }

    public static class OptionString extends Faker<String> {
        private final String[] options;
        private final boolean random;
        private int index = 0;

        public OptionString(String[] options, boolean random) {
            this.options = options;
            this.random = random;
        }

        @Override
        public String geneValue() throws Exception {
            if(options.length == 0){
                return null;
            } else if (options.length == 1) {
                return options[0];
            } else{
                if (!random) {
                    if (index == options.length) {
                        index = 0;
                    }
                    return options[index++];
                } else {
                    return options[ThreadLocalRandom.current().nextInt(options.length)];
                }
            }
        }
    }
}
