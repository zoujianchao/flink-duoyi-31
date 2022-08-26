package cn.doitedu.day10;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 异步IO的使用场景：（用来查询外部的数据的）
 * 访问大量的数据，这些数据无法直接拿到（请求数据库、外部的接口），并希望关联的速度更快，提高程序额吞吐量
 * <p>
 * 异步IO底层的原理：
 * 底层就是使用多线程的方式，在一段时间内，使用多线程（线程池）方式在同一个subtask发送更多的请求（异步的）
 * <p>
 * 代价：消耗的更多的CPU资源
 * <p>
 * 该例子使用异步IO请求高德地图逆地理位置API，获取相关的信息
 */
public class C01_AsyncIODemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //测试数据
        //{"oid": "o1000", "cid": "c10", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        //{"oid": "o1001", "cid": "c11", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        //{"oid": "o1000", "cid": "c10", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        //{"oid": "o1001", "cid": "c11", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        //{"oid": "o1000", "cid": "c10", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        //{"oid": "o1001", "cid": "c11", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<OrderBean> beanStream = lines.process(new ProcessFunction<String, OrderBean>() {
            @Override
            public void processElement(String line, Context ctx, Collector<OrderBean> out) throws Exception {
                try {
                    OrderBean orderBean = JSON.parseObject(line, OrderBean.class);
                    out.collect(orderBean);
                } catch (Exception e) {
                    //e.printStackTrace();
                    //记录有有问题的数据
                }
            }
        });

        //使用异步IO关联要查询的数据
        SingleOutputStreamOperator<OrderBean> res = AsyncDataStream.unorderedWait(beanStream, new HttpAsyncFunction(), 3000, TimeUnit.MILLISECONDS);

        res.print();

        env.execute();

    }

    /**
     * 在pom文件中添加异步httpClient的依赖
     * <p>
     * <!-- 高效的异步HttpClient -->
     * <dependency>
     * <groupId>org.apache.httpcomponents</groupId>
     * <artifactId>httpasyncclient</artifactId>
     * <version>4.1.4</version>
     * </dependency>
     */
    public static class HttpAsyncFunction extends RichAsyncFunction<OrderBean, OrderBean> {

        String key = "74e962e2f795114980cd33ce09d923d1";

        private CloseableHttpAsyncClient httpclient;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化可以多线程发送异步请求的客户端
            //创建异步查询的HTTPClient
            //创建一个异步的HttpClient连接池
            //初始化异步的HttpClient
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(3000)
                    .setConnectTimeout(3000)
                    .build();
            httpclient = HttpAsyncClients.custom()
                    .setMaxConnTotal(20)
                    .setDefaultRequestConfig(requestConfig)
                    .build();
            //开启异步查询的线程池
            httpclient.start();
        }

        //asyncInvoke方法也是来一条调用一次
        //在该方法中可以开多线程进行查询，不必等待该方法的返回，就可以对下一条数据进行异步查询
        @Override
        public void asyncInvoke(OrderBean orderBean, ResultFuture<OrderBean> resultFuture) throws Exception {

            //获取查询条件
            try {
                double longitude = orderBean.longitude;
                double latitude = orderBean.latitude;
                //使用Get方式进行查询
                HttpGet httpGet = new HttpGet("https://restapi.amap.com/v3/geocode/regeo?&location=" + longitude + "," + latitude + "&key=" + key);
                //查询返回Future
                Future<HttpResponse> future = httpclient.execute(httpGet, null);

                //从Future中取数据（回调方法）
                CompletableFuture.supplyAsync(new Supplier<OrderBean>() {
                    //当Future中有返回的数据，会调用get方法
                    @Override
                    public OrderBean get() {
                        try {
                            //从future中取数据
                            HttpResponse response = future.get();
                            String province = null;
                            String city = null;
                            //获取查询相应状态
                            if (response.getStatusLine().getStatusCode() == 200) {
                                //获取请求的json字符串
                                String result = EntityUtils.toString(response.getEntity());
                                //System.out.println(result);
                                //转成json对象
                                JSONObject jsonObj = JSON.parseObject(result);
                                //获取位置信息
                                JSONObject regeocode = jsonObj.getJSONObject("regeocode");
                                if (regeocode != null && !regeocode.isEmpty()) {
                                    JSONObject address = regeocode.getJSONObject("addressComponent");
                                    //获取省市区
                                    province = address.getString("province");
                                    city = address.getString("city");
                                    //String businessAreas = address.getString("businessAreas");
                                }
                            }
                            orderBean.province = province;
                            orderBean.city = city;
                            return orderBean;
                        } catch (Exception e) {
                            // Normally handled explicitly.
                            return null;
                        }
                    }
                }).thenAccept((OrderBean result) -> {
                    resultFuture.complete(Collections.singleton(result));
                });

            } catch (Exception e) {
                resultFuture.complete(Collections.singleton(null));
            }

        }
    }

    public static class OrderBean {

        public String oid;

        public String cid;

        public Double money;

        public Double longitude;

        public Double latitude;

        public String province;

        public String city;

        public OrderBean() {
        }

        public OrderBean(String oid, String cid, Double money, Double longitude, Double latitude) {
            this.oid = oid;
            this.cid = cid;
            this.money = money;
            this.longitude = longitude;
            this.latitude = latitude;
        }

        public static OrderBean of(String oid, String cid, Double money, Double longitude, Double latitude) {
            return new OrderBean(oid, cid, money, longitude, latitude);
        }

        @Override
        public String toString() {
            return "OrderBean{" + "oid='" + oid + '\'' + ", cid='" + cid + '\'' + ", money=" + money + ", longitude=" + longitude + ", latitude=" + latitude + ", province='" + province + '\'' + ", city='" + city + '\'' + '}';
        }

    }

}
