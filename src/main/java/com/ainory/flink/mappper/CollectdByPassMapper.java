package com.ainory.flink.mappper;

import com.ainory.flink.entity.AggreagationVO;
import com.ainory.flink.entity.CollectdKafkaVO;
import com.ainory.flink.util.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author ainory on 2018. 4. 3..
 */
public class CollectdByPassMapper implements MapFunction<String, String> {

    private static final long serialVersionUID = 1180234853172462378L;

    @Override
    public String map(String message) throws Exception {

        System.out.println("2 -> " + message);

        CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(message, CollectdKafkaVO[].class);

        String returnMsg = arrCollectdKafkaVO[0].toString();
        for(CollectdKafkaVO collectdKafkaVO : arrCollectdKafkaVO){

            System.out.println(collectdKafkaVO.toString());

        }
        return message;
    }

}
