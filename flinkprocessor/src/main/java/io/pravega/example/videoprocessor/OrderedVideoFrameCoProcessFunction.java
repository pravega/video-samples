package io.pravega.example.videoprocessor;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Optional;

class OrderedVideoFrameCoProcessFunction extends CoProcessFunction<OrderedVideoFrame, OrderedVideoFrame, OrderedVideoFrame> {
    private static Logger log = LoggerFactory.getLogger(OrderedVideoFrameCoProcessFunction.class);

    private ValueState<Long> nextOutputIndexState;
    private MapState<Long, OrderedVideoFrame> outOfOrderElementsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        nextOutputIndexState = getRuntimeContext().getState(new ValueStateDescriptor<>("nextFrameNumberToEmit", Long.class));
        outOfOrderElementsState = getRuntimeContext().getMapState(new MapStateDescriptor<>("outOfOrderElements", Long.class, OrderedVideoFrame.class));
    }

    @Override
    public void processElement1(OrderedVideoFrame value, Context ctx, Collector<OrderedVideoFrame> out) throws Exception {
        processElementHelper(value, ctx, out);
    }

    @Override
    public void processElement2(OrderedVideoFrame value, Context ctx, Collector<OrderedVideoFrame> out) throws Exception {
        processElementHelper(value, ctx, out);
    }

    private void processElementHelper(OrderedVideoFrame value, Context ctx, Collector<OrderedVideoFrame> out) throws Exception {
        long nextOutputIndex = Optional.ofNullable(nextOutputIndexState.value()).orElse(0L);
        if (nextOutputIndex == value.index) {
            // We can emit this element now.
            out.collect(value);
            nextOutputIndex++;
            // Emit buffered elements.
            for(;;) {
                final OrderedVideoFrame buffered = outOfOrderElementsState.get(nextOutputIndex);
                if (buffered == null) {
                    break;
                }
                // Emit the buffered element now.
                log.info("CLAUDIO Emitting buffered element; nextOutputIndex={}, buffered={}", nextOutputIndex, buffered);
                out.collect(buffered);
                outOfOrderElementsState.remove(nextOutputIndex);
                nextOutputIndex++;
            }
            nextOutputIndexState.update(nextOutputIndex);
        } else if (nextOutputIndex < value.index) {
            // Add early element to buffer.
            log.info("CLAUDIO Buffering early element; nextOutputIndex={}, value={}", nextOutputIndex, value);
            outOfOrderElementsState.put(value.index, value);
        } else {
            throw new IllegalStateException(MessageFormat.format("Unexpected element order; nextOutputIndex={0}, value.index={1}", nextOutputIndex, value.index));
        }
    }
}
