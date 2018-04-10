package com.ainory.flink.tests;

import com.ainory.flink.mappper.CollectdByPassMapper;
import com.ainory.flink.mappper.CollectdMapper2;
import com.ainory.flink.watermark.CustomWatermarkExtractor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.math.BigDecimal;

/**
 * @author ainory on 2018. 4. 3..
 */
public class FlinkStreamTest {

    /**
     * collectd -> kafka -> flink(stream) -> ohter topic
     *
     * <p>arguments :
     * 	--input-topic COLLECTD_DATA --output-topic ainory-flink-stream-1 --bootstrap.servers spanal-app:9092 --zookeeper.connect spanal-app:2181/kafka --group.id ainory-consummer --auto.offset.reset latest
     *
     * @param args
     */
    public void ByPassStreamTest(String[] args){
        try{
            final ParameterTool parameterTool = ParameterTool.fromArgs(args);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//            env.getConfig().disableSysoutLogging();
            env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
            env.enableCheckpointing(5000);
            env.getConfig().setGlobalJobParameters(parameterTool);

            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStream<String> input = env.addSource(new FlinkKafkaConsumer010<>(parameterTool.getRequired("input-topic"),
                    new SimpleStringSchema(),
                    parameterTool.getProperties()))
                    .assignTimestampsAndWatermarks(new CustomWatermarkExtractor())
                    .map(new CollectdByPassMapper());

            input.addSink(new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic"),
                    new SimpleStringSchema(),
                    parameterTool.getProperties()));

            env.execute("ByPassStreamTest for ainory");

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void WindowStreamTest(String[] args){
        try{
            final ParameterTool parameterTool = ParameterTool.fromArgs(args);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//            env.getConfig().disableSysoutLogging();
            env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
            env.enableCheckpointing(5000);
            env.getConfig().setGlobalJobParameters(parameterTool);

            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            /**
             * source and mapper
             */
            DataStream<Tuple2<String, Double>> input = env.addSource(new FlinkKafkaConsumer010<>(parameterTool.getRequired("input-topic"),
                                                                     new SimpleStringSchema(),
                                                                     parameterTool.getProperties()))
                                                  .assignTimestampsAndWatermarks(new CustomWatermarkExtractor())
                                                  .map(new CollectdMapper2());

            /**
             *  min only aggregation sink
             */
            /*
            input.keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                    .min(1)
                    .map(new MapFunction<Tuple2<String, Double>, String>() {
                            @Override
                            public String map(Tuple2<String, Double> stringDoubleTuple2) throws Exception {

                                StringBuffer result = new StringBuffer();

                                result.append(stringDoubleTuple2.f0).append(" ==> ").append(stringDoubleTuple2.f1.toString());

                                return result.toString();
                            }
                        })
                    .addSink(new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic"),
                             new SimpleStringSchema(),
                             parameterTool.getProperties()));
            */

            /**
             *  min/max/sum aggregation sink
             */
            // min
            SingleOutputStreamOperator<Tuple2<String, Double>> min = input.keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                    .min(1);

            min.map(new MapFunction<Tuple2<String, Double>, String>() {
                @Override
                public String map(Tuple2<String, Double> stringDoubleTuple2) throws Exception {

                    StringBuffer result = new StringBuffer();

                    result.append(stringDoubleTuple2.f0).append("_min").append(" ==> ").append(stringDoubleTuple2.f1.toString());

                    return result.toString();
                }
            }).addSink(new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic"),
                    new SimpleStringSchema(),
                    parameterTool.getProperties()));

            // max
            SingleOutputStreamOperator<Tuple2<String, Double>> max = input.keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                    .max(1);

            max.map(new MapFunction<Tuple2<String, Double>, String>() {
                @Override
                public String map(Tuple2<String, Double> stringDoubleTuple2) throws Exception {

                    StringBuffer result = new StringBuffer();

                    result.append(stringDoubleTuple2.f0).append("_max").append(" ==> ").append(stringDoubleTuple2.f1.toString());

                    return result.toString();
                }
            }).addSink(new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic"),
                    new SimpleStringSchema(),
                    parameterTool.getProperties()));

            // sum
            SingleOutputStreamOperator<Tuple2<String, Double>> sum = input.keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                    .sum(1);

            sum.map(new MapFunction<Tuple2<String, Double>, String>() {
                @Override
                public String map(Tuple2<String, Double> stringDoubleTuple2) throws Exception {

                    StringBuffer result = new StringBuffer();

                    result.append(stringDoubleTuple2.f0).append("_sum").append(" ==> ").append(stringDoubleTuple2.f1.toString());

                    return result.toString();
                }
            }).addSink(new FlinkKafkaProducer010<>(parameterTool.getRequired("output-topic"),
                    new SimpleStringSchema(),
                    parameterTool.getProperties()));

            env.execute("WindowStreamTest for ainory");


        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        FlinkStreamTest flinkStreamTest = new FlinkStreamTest();

//        flinkStreamTest.ByPassStreamTest(args);
        flinkStreamTest.WindowStreamTest(args);
    }
}
