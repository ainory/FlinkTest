package com.ainory.flink.mappper;

import com.ainory.flink.entity.AggreagationVO;
import com.ainory.flink.entity.CollectdKafkaVO;
import com.ainory.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * @author ainory on 2018. 4. 3..
 */
public class CollectdMapper implements MapFunction<String, AggreagationVO> {

    private static final long serialVersionUID = 1180234853172462378L;

    @Override
    public AggreagationVO map(String message) {

        CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(message, CollectdKafkaVO[].class);

        CollectdKafkaVO collectdKafkaVO = arrCollectdKafkaVO[0];

        AggreagationVO aggreagationVO = new AggreagationVO();

        aggreagationVO.setHostname(collectdKafkaVO.getHost());
        aggreagationVO.setPlugin(collectdKafkaVO.getPlugin());
        aggreagationVO.setPlugin_instance(collectdKafkaVO.getPlugin_instance());
        aggreagationVO.setType(collectdKafkaVO.getType());
        aggreagationVO.setType_instance(collectdKafkaVO.getType_instance());


        StringBuffer resultKey = new StringBuffer();
        if (collectdKafkaVO.getDsnames().get(0).equals("value")) {
            // make key - single value ( ex -> [{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1522299234.188,"interval":10.000,"host":"spanal-3","plugin":"disk","plugin_instance":"dm-2","type":"pending_operations","type_instance":"","meta":{"network:received":true}}] )
            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());

            if (collectdKafkaVO.getValues().get(0) instanceof Integer) {
                aggreagationVO.setValue(new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Long) {
                aggreagationVO.setValue(new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Float) {
                aggreagationVO.setValue(new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Double) {
                aggreagationVO.setValue(new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0))));
            } else {
                aggreagationVO.setValue(new BigDecimal(0));
            }


        } else {
            // make key - multi value ( ex -> [{"values":[0,0.699869398389539],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1522299234.188,"interval":10.000,"host":"spanal-3","plugin":"disk","plugin_instance":"dm-2","type":"disk_time","type_instance":"","meta":{"network:received":true}}] )
            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType());
            resultKey.append("_").append(collectdKafkaVO.getDsnames().get(0));

            if (collectdKafkaVO.getValues().get(0) instanceof Integer) {
                aggreagationVO.setValue(new BigDecimal(new Integer((Integer) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Long) {
                aggreagationVO.setValue(new BigDecimal(new Long((Long) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Float) {
                aggreagationVO.setValue(new BigDecimal(new Float((Float) collectdKafkaVO.getValues().get(0))));
            } else if (collectdKafkaVO.getValues().get(0) instanceof Double) {
                aggreagationVO.setValue(new BigDecimal(new Double((Double) collectdKafkaVO.getValues().get(0))));
            } else {
                aggreagationVO.setValue(new BigDecimal(0));
            }
        }

        aggreagationVO.setKey(resultKey.toString());

        return aggreagationVO;
    }

    /*@Override
    public String map(String message) throws Exception {

        System.out.println("2 -> " + message);

        CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(message, CollectdKafkaVO[].class);

        String returnMsg = arrCollectdKafkaVO[0].toString();
        *//*for(CollectdKafkaVO collectdKafkaVO : arrCollectdKafkaVO){

            System.out.println(collectdKafkaVO.toString());

        }*//*
        return message;
    }*/

}
