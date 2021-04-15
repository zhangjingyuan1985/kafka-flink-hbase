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
import scala.tools.nsc.transform.patmat.ScalaLogic;
import org.apache.flink.configuration.Configuration;


import java.io.IOException;
import java.util.Properties;


public class flink2redis2 {
    private static TableName tableName = TableName.valueOf("Flink2HBase");
    private static final String columnFamily = "info";
    public static double total=0;

    public static void main(String[] args) throws Exception {

        final String ZOOKEEPER_HOST = "172.17.8.37:2181";
        final String KAFKA_HOST = "172.17.8.37:9092";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000); // 非常关键，一定要设置启动检查点！！
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.setProperty("zookeeper.connect", ZOOKEEPER_HOST);
        props.setProperty("bootstrap.servers", KAFKA_HOST);
        props.setProperty("group.id", "test1");

        DataStream<String> transction = env.addSource(new FlinkKafkaConsumer010<String>("haha", new SimpleStringSchema(), props)).setParallelism(1).name("source_kafka_trade").uid("source_kafka_trade");


        DoubleCounter numLines = new DoubleCounter();

        transction.rebalance().map(new RichMapFunction<String, Object>() {

            private static final long serialVersionUID = 1L;
            private IntCounter numLines = new IntCounter();
            @Override
            public void open(Configuration parameters) throws Exception{
                super.open(parameters);
                getRuntimeContext().addAccumulator("numLines", this.numLines);

            }

            @Override
            public String map(String value)throws IOException {
            this.numLines.add(1);

              //  JSONObject jsonobj = JSON.parseObject(value);
                System.out.println(value);

      /*          if(jsonobj.getIntValue("tid") == 65976) {
                //    System.out.println(value);
                    String instrumentID = jsonobj.getJSONObject("body").getString("instrumentID");
                    String tradingDay = jsonobj.getJSONObject("body").getString("tradingDay");
                    double tradePrice = jsonobj.getJSONObject("body").getDoubleValue("tradePrice");
                    int tradeVolume = jsonobj.getJSONObject("body").getIntValue("tradeVolume");
                    double sum = tradeVolume * tradePrice; */
                  total = total + Double.parseDouble(value);
                   /* System.out.println(value);
                    System.out.println(sum);
                    System.out.println(total);}   */


                System.out.println(total);

                    //              writeIntoHBase(value);
                    return null;

                }

        });




        env.execute();
    }



/*     public static void writeIntoredis(String m)throws IOException{
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