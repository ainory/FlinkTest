package com.ainory.flink.watermark;

import com.ainory.flink.entity.CollectdKafkaVO;
import com.ainory.flink.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<String> {

    private static final long serialVersionUID = -742759155861320823L;

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(String element, long previousElementTimestamp) {

        try{
            CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(element, CollectdKafkaVO[].class);

            long timestamp = Long.parseLong(StringUtils.replace(arrCollectdKafkaVO[0].getTime(), ".",""));

            this.currentTimestamp = timestamp;

            return timestamp;
        }catch (Exception e){
            e.printStackTrace();

            return this.currentTimestamp;
        }
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
    }
}