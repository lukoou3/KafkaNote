package com.kafka.faker;

import java.util.concurrent.ThreadLocalRandom;

public interface NumberFaker {
    public static class RangeIntNumber extends Faker<Integer> {
        private final int start;
        private final int end;
        private final boolean random;
        private final boolean oneValue;
        private int value;

        public RangeIntNumber(int start, int end, boolean random) {
            this.start = start;
            this.end = end;
            this.random = random;
            this.oneValue = start + 1 == end;
            this.value = start;
        }

        @Override
        public Integer geneValue() throws Exception {
            if (oneValue) {
                return start;
            }
            if (random) {
                return ThreadLocalRandom.current().nextInt(start, end);
            } else {
                if (value == end) {
                    value = start;
                }
                return value++;
            }
        }
    }

    public static class RangeLongNumber extends Faker<Long> {
        private final long start;
        private final long end;
        private final boolean random;
        private final boolean oneValue;
        private long value;

        public RangeLongNumber(long start, long end, boolean random) {
            this.start = start;
            this.end = end;
            this.random = random;
            this.oneValue = start + 1 == end;
            this.value = start;
        }

        @Override
        public Long geneValue() throws Exception {
            if (oneValue) {
                return start;
            }
            if (random) {
                return ThreadLocalRandom.current().nextLong(start, end);
            } else {
                if (value == end) {
                    value = start;
                }
                return value++;
            }
        }
    }

    public static class RangeDoubleNumber extends Faker<Double> {
        private double start;
        private double end;

        public RangeDoubleNumber(double start, double end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public Double geneValue() throws Exception {
            return ThreadLocalRandom.current().nextDouble(start, end);
        }
    }

    public static class OptionIntNumber extends Faker<Integer> {
        private final Integer[] options;
        private final boolean random;
        private int index = 0;

        public OptionIntNumber(Integer[] options, boolean random) {
            this.options = options;
            this.random = random;
        }

        @Override
        public Integer geneValue() throws Exception {
            if (options.length == 0) {
                return null;
            } else if (options.length == 1) {
                return options[0];
            } else {
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

    public static class OptionLongNumber extends Faker<Long> {
        private final Long[] options;
        private final boolean random;
        private int index = 0;

        public OptionLongNumber(Long[] options, boolean random) {
            this.options = options;
            this.random = random;
        }

        @Override
        public Long geneValue() throws Exception {
            if (options.length == 0) {
                return null;
            } else if (options.length == 1) {
                return options[0];
            } else {
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

    public static class OptionDoubleNumber extends Faker<Double> {
        private final Double[] options;
        private final boolean random;
        private int index = 0;

        public OptionDoubleNumber(Double[] options, boolean random) {
            this.options = options;
            this.random = random;
        }

        @Override
        public Double geneValue() throws Exception {
            if (options.length == 0) {
                return null;
            } else if (options.length == 1) {
                return options[0];
            } else {
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
