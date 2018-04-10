package com.ainory.flink.mappper;

import com.ainory.flink.entity.CollectdKafkaVO;
import com.ainory.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;

/**
 * @author ainory on 2018. 4. 3..
 */
public class CollectdMapper2 implements MapFunction<String, Tuple2<String, Double>> {

    private static final long serialVersionUID = 1180234853172462378L;

    @Override
    public Tuple2<String, Double> map(String message) {

        CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(message, CollectdKafkaVO[].class);

        CollectdKafkaVO collectdKafkaVO = arrCollectdKafkaVO[0];

        StringBuffer resultKey = new StringBuffer();
        BigDecimal resultValue = null;
        Double aDouble = 0.0D;

        if (collectdKafkaVO.getDsnames().get(0).equals("value")) {
            // make key - single value ( ex -> [{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1522299234.188,"interval":10.000,"host":"spanal-3","plugin":"disk","plugin_instance":"dm-2","type":"pending_operations","type_instance":"","meta":{"network:received":true}}] )
            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());

            /*if (collectdKafkaVO.getValues().get(0) instanceof Integer) {
                resultValue = new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0)));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Long) {
                resultValue = new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0)));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Float) {
                resultValue = new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0)));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Double) {
                resultValue = new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0)));
            } else {
                resultValue = new BigDecimal(0);
            }*/

        } else {
            // make key - multi value ( ex -> [{"values":[0,0.699869398389539],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1522299234.188,"interval":10.000,"host":"spanal-3","plugin":"disk","plugin_instance":"dm-2","type":"disk_time","type_instance":"","meta":{"network:received":true}}] )
            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType());
            resultKey.append("_").append(collectdKafkaVO.getDsnames().get(0));

            /*if (collectdKafkaVO.getValues().get(0) instanceof Integer) {
                resultValue = new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0)));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Long) {
                resultValue = new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0)));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Float) {
                resultValue = new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0)));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Double) {
                resultValue = new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0)));
            } else {
                resultValue = new BigDecimal(0);
            }*/
        }

        aDouble = Double.parseDouble(String.valueOf(collectdKafkaVO.getValues().get(0)));

        System.out.println(resultKey.toString() + " => " + aDouble);

        return new Tuple2<>(resultKey.toString(), aDouble);
    }
}
