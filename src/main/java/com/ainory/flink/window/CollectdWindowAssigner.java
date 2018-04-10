package com.ainory.flink.window;

import com.ainory.flink.entity.AggreagationVO;
import com.ainory.flink.entity.AggregationWindowVO;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import java.util.Collection;

/**
 * @author ainory on 2018. 4. 4..
 */
public class CollectdWindowAssigner extends WindowAssigner<AggreagationVO, AggregationWindowVO>{
    @Override
    public Collection<AggregationWindowVO> assignWindows(AggreagationVO aggreagationVO, long l, WindowAssignerContext windowAssignerContext) {
        return null;
    }

    @Override
    public Trigger<AggreagationVO, AggregationWindowVO> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return null;
    }

    @Override
    public TypeSerializer<AggregationWindowVO> getWindowSerializer(ExecutionConfig executionConfig) {
        return null;
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
