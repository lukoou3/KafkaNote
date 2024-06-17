package com.kafka.faker;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public interface TimestampFaker {


    public static class UnixTimestamp extends Faker<Long> {

        @Override
        public Long geneValue() throws Exception {
            return System.currentTimeMillis() / 1000;
        }
    }

    public static class Timestamp extends Faker<Long> {

        @Override
        public Long geneValue() throws Exception {
            return System.currentTimeMillis();
        }
    }

    public static class FormatTimestamp extends Faker<String> {
        // 标准日期时间格式，精确到秒：yyyy-MM-dd HH:mm:ss
        public static final String NORM_DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
        public static final DateTimeFormatter NORM_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(NORM_DATETIME_PATTERN);
        // 标准日期时间格式，精确到毫秒：yyyy-MM-dd HH:mm:ss.SSS
        public static final String NORM_DATETIME_MS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
        public static final DateTimeFormatter NORM_DATETIME_MS_FORMATTER = DateTimeFormatter.ofPattern(NORM_DATETIME_MS_PATTERN);

        private final String format;
        private transient DateTimeFormatter formatter;
        private final ZoneId zone;


        public FormatTimestamp() {
            this(NORM_DATETIME_PATTERN, false);
        }

        public FormatTimestamp(String format, boolean utc) {
            assert format != null;
            this.format = format;
            this.zone = utc ? ZoneId.of("UTC") : ZoneId.systemDefault();
            switch (format) {
                case NORM_DATETIME_PATTERN:
                    this.formatter = NORM_DATETIME_FORMATTER;
                    break;
                case NORM_DATETIME_MS_PATTERN:
                    this.formatter = NORM_DATETIME_MS_FORMATTER;
                    break;
                default:
                    this.formatter = DateTimeFormatter.ofPattern(format);
            }

        }

        @Override
        public void init(int instanceCount, int instanceIndex) throws Exception {
            super.init(instanceCount, instanceIndex);
            switch (format) {
                case NORM_DATETIME_PATTERN:
                    this.formatter = NORM_DATETIME_FORMATTER;
                    break;
                case NORM_DATETIME_MS_PATTERN:
                    this.formatter = NORM_DATETIME_MS_FORMATTER;
                    break;
                default:
                    this.formatter = DateTimeFormatter.ofPattern(format);
            }
        }

        @Override
        public String geneValue() throws Exception {
            LocalDateTime dateTime = LocalDateTime.now(zone);
            return dateTime.format(formatter);
        }
    }
}
