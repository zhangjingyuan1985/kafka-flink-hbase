package com.du;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.hadoop.hbase.TableName;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;


public class flink2redis1 {
    private static TableName tableName = TableName.valueOf("Flink2HBase");
    private static final String columnFamily = "info";
    public static double total=0;


    public static void main(String[] args) throws Exception {


        final String ZOOKEEPER_HOST = "172.16.13.223:2181";
        final String KAFKA_HOST = "172.16.13.223:9092";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000); // 非常关键，一定要设置启动检查点！！
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.setProperty("zookeeper.connect", ZOOKEEPER_HOST);
        props.setProperty("bootstrap.servers", KAFKA_HOST);
        props.setProperty("group.id", "test1");

        DataStream<String> transction = env.addSource(new FlinkKafkaConsumer010<String>("trade", new SimpleStringSchema(), props)).setParallelism(1).name("source_kafka_trade").uid("source_kafka_trade");


        DoubleCounter numLines = new DoubleCounter();

        transction.rebalance().map(new RichMapFunction<String, Object>() {
            HashMap<String,Double> map1 = new HashMap<String, Double>();
            double total=0;
            private static final long serialVersionUID = 1L;
            IntCounter numLines = new IntCounter();

            public String map(String value)throws IOException {

                JSONObject jsonobj = JSON.parseObject(value);
                String tradingDay = jsonobj.getJSONObject("body").getString("tradingDay");
                double tradePrice = jsonobj.getJSONObject("body").getDoubleValue("tradePrice");
                int tradeVolume = jsonobj.getJSONObject("body").getIntValue("tradeVolume");
                double sum = tradeVolume * tradePrice;
                if(jsonobj.getIntValue("tid") == 65976) {
                //    System.out.println(value);


                    String instrumentID = jsonobj.getJSONObject("body").getString("instrumentID");
                    String code=instrumentID.replaceAll("\\d+","");

                    if(map1.containsKey(code+tradingDay)){
                        map1.put(code+tradingDay,map1.get(code+tradingDay)+sum);
                        System.out.println(value);
                        System.out.println(sum);
                        String json="{var_code:"+code+",tradingDay:"+tradingDay+",total:"+map1.get(code+tradingDay)+"}";
                        writeIntoredis(code+'_'+tradingDay,json);
                    }
                    else {map1.put(code+tradingDay,sum);

                        System.out.println(value);
                        System.out.println(sum);
                        String json="{var_code:"+code+",tradingDay:"+tradingDay+",total:"+sum+"}";
                        writeIntoredis(code+'_'+tradingDay,json);
                    }





             /*       Set entries = map1.entrySet();
                    Iterator iter = entries.iterator();
                    while(iter.hasNext())
                    {
                        Map.Entry entry = (Map.Entry)iter.next();
                        String key = entry.getKey();//得到键
                        String value = entry.getValue();//得到值
                    }    */




                    /*
                    if(instrumentID =="TA107"){
                    total_TA107 = sum + total;
                    System.out.println(value);
                    System.out.println(sum);
                    System.out.println(total);
                    String json="{instrumentID:"+instrumentID+",tradingDay:"+tradingDay+",total:"+total+"}";
                    writeIntoredis(instrumentID,json);}

                    else if(instrumentID =="TA105"){
                        total_TA105 = sum + total;
                        System.out.println(value);
                        System.out.println(sum);
                        System.out.println(total);
                        String json="{instrumentID:"+instrumentID+",tradingDay:"+tradingDay+",total:"+total+"}";
                        writeIntoredis(instrumentID,json);
                    }

                    else if(instrumentID =="cu2105P63000"){
                        total_TA106 = sum + total;
                        System.out.println(value);
                        System.out.println(sum);
                        System.out.println(total);
                        String json="{instrumentID:"+instrumentID+",tradingDay:"+tradingDay+",total:"+total+"}";
                        writeIntoredis(instrumentID,json);
                    }

                    else if(instrumentID =="TA106"){
                        total_TA106 = sum + total;
                        System.out.println(value);
                        System.out.println(sum);
                        System.out.println(total);
                        String json="{instrumentID:"+instrumentID+",tradingDay:"+tradingDay+",total:"+total+"}";
                        writeIntoredis(instrumentID,json);
                    }
                    else if(instrumentID =="ag2104"){
                        total_TA106 = sum + total;
                        System.out.println(value);
                        System.out.println(sum);
                        System.out.println(total);
                        String json="{instrumentID:"+instrumentID+",tradingDay:"+tradingDay+",total:"+total+"}";
                        writeIntoredis(instrumentID,json);
                    }

                    else if(instrumentID =="ag2105"){
                        total_TA106 = sum + total;
                        System.out.println(value);
                        System.out.println(sum);
                        System.out.println(total);
                        String json="{instrumentID:"+instrumentID+",tradingDay:"+tradingDay+",total:"+total+"}";
                        writeIntoredis(instrumentID,json);
                    }
                    else if(instrumentID =="ag2106"){
                        total_TA106 = sum + total;
                        System.out.println(value);
                        System.out.println(sum);
                        System.out.println(total);
                        String json="{instrumentID:"+instrumentID+",tradingDay:"+tradingDay+",total:"+total+"}";
                        writeIntoredis(instrumentID,json);
                    }
                    else if(instrumentID =="ag2107"){
                        total_TA106 = sum + total;
                        System.out.println(value);
                        System.out.println(sum);
                        System.out.println(total);
                        String json="{instrumentID:"+instrumentID+",tradingDay:"+tradingDay+",total:"+total+"}";
                        writeIntoredis(instrumentID,json);
                    }

                         */








                }




                    //
                    return null;

                }

        });




        env.execute();
    }



    public static void writeIntoredis(String key,String value)throws IOException{

        Jedis jedis = new Jedis("172.17.8.37");
        jedis.set(key,value);


     }






    /**
    public static void writeIntoHBase(String m)throws IOException{
        ConnectionPoolConfig config = new ConnectionPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(true);


        Configuration hbaseConfig = HBaseConfiguration.create();

        hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", "node71:2181,node72:2181,node73:2181");
        hbaseConfig.set("hbase.defaults.for.version.skip", "true");

        HbaseConnectionPool pool = null;

        try {
            pool = new HbaseConnectionPool(config, hbaseConfig);

            Connection con = pool.getConnection();

            Admin admin = con.getAdmin();

            if(!admin.tableExists(tableName)){
                admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(columnFamily)));
            }
            Table table = con.getTable(tableName);

            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            Put put = new Put(org.apache.hadoop.hbase.util.Bytes.toBytes(df.format(new Date())));

            put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes(columnFamily), org.apache.hadoop.hbase.util.Bytes.toBytes("test"),
                    org.apache.hadoop.hbase.util.Bytes.toBytes(m));

            table.put(put);
            table.close();
            pool.returnConnection(con);

        } catch (Exception e) {
            pool.close();
        }
    }
     **/
}