package com.kafka.faker;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class IPv4Faker extends Faker<String> {
    private final long start;
    private final long end;
    private StringBuilder sb = new StringBuilder(16);

    public IPv4Faker(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public String geneValue() throws Exception {
        return longToIp(ThreadLocalRandom.current().nextLong(start, end));
    }

    private String longToIp(long ipLong) {
        //sb.delete(0, sb.length());
        sb.setLength(0);

        sb.append(ipLong >>> 24).append(".");
        sb.append((ipLong >>> 16) & 0xFF).append(".");
        sb.append((ipLong >>> 8) & 0xFF).append(".");
        sb.append(ipLong & 0xFF);

        return sb.toString();
    }

    public static long ipv4ToLong(String ipStr) {
        long[] temp = Arrays.stream(ipStr.split("\\.")).mapToLong(x -> Long.parseLong(x)).toArray();
        Preconditions.checkArgument(temp.length == 4);
        for (long l : temp) {
            Preconditions.checkArgument(l >= 0 && l <= 255);
        }
        long ipLong = (temp[0] << 24) + (temp[1] << 16) + (temp[2] << 8) + temp[3];
        return ipLong;
    }
}
