package cn.doitedu.day04;

public class WindowRangeTest {

    public static void main(String[] args) {

        //窗口的起始时间，结束时间[) ，必须是窗口长度的整数倍
        long inputTime = 11003;
        long windowSize = 5000;

        long startTime = inputTime - (inputTime % windowSize);
        long endTime = inputTime - (inputTime % windowSize) + windowSize;

        System.out.println("[" + startTime + "," + endTime + ")");

    }

}
