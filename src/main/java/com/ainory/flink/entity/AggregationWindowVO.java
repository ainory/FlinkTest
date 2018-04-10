package com.ainory.flink.entity;

import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * @author ainory on 2018. 4. 4..
 */
public class AggregationWindowVO extends Window {
    @Override
    public long maxTimestamp() {
        return 0;
    }
}
