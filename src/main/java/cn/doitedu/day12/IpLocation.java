package cn.doitedu.day12;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 在Flink中，实现自定义UDF（输入一条，返回一条），需要继承ScalarFunction
 */
public class IpLocation extends ScalarFunction {

    private List<Tuple4<Long, Long, String, String>> lines = new ArrayList<>();

    //open方法是生命周期方法，会在调用evel方法前，调用一次
    @Override
    public void open(FunctionContext context) throws Exception {

        //获取缓存的文件（在TaskManager中获取缓存的数据）
        File cachedFile = context.getCachedFile("ip-rules");

        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(cachedFile)));

        String line = null;
        //将数据整理好，添加到ArrayList（内存中了）
        while ((line = bufferedReader.readLine()) != null) {
            String[] fields = line.split("[|]");
            Long startNum = Long.parseLong(fields[2]);
            Long endNum = Long.parseLong(fields[3]);
            String province = fields[6];
            String city = fields[7];
            lines.add(Tuple4.of(startNum, endNum, province, city));
        }
    }

    //方法名必须叫eval
    public String eval(String ip) {
        Long ipNum = ip2Long(ip);
        return binarySearch(ipNum);
    }


//    public TypeInformation<Row> getResultType() {
//        return Types.ROW_NAMED(new String[]{"province", "city"}, Types.STRING, Types.STRING);
//    }

    private Long ip2Long(String ip) {
        String[] fragments = ip.split("[.]");
        Long ipNum = 0L;
        for (int i = 0; i < fragments.length; i++) {
            ipNum = Long.parseLong(fragments[i]) | ipNum << 8L;
        }
        return ipNum;
    }

    /**
     * 二分法查找
     */
    private String binarySearch(Long ipNum) {

        //Row result = null;
        String result = null;
        int index = -1;
        int low = 0;//起始
        int high = lines.size() - 1; //结束

        while (low <= high) {
            int middle = (low + high) / 2;
            if ((ipNum >= lines.get(middle).f0) && (ipNum <= lines.get(middle).f1))
                index = middle;
            if (ipNum < lines.get(middle).f0)
                high = middle - 1;
            else {
                low = middle + 1;
            }
        }
        if (index != -1) {
            Tuple4<Long, Long, String, String> tp4 = lines.get(index);
            //result = Row.of(tp4.f2, tp4.f3);
            result = tp4.f2;
        }
        return result;
    }


}
