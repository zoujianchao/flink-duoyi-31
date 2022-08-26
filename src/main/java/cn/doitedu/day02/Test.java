package cn.doitedu.day02;

import org.apache.flink.util.MathUtils;

public class Test {

    public static void main(String[] args) {

        String s = "spark";
        int code = s.hashCode();
        int i = code % 4;

        System.out.println(i);

        int j= MathUtils.murmurHash(code) % 128 * 4 / 128;

        System.out.println(j);

        System.out.println(129 / 128);


    }
}
