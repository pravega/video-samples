package io.pravega.example.videoprocessor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

public class OrderedParallelOperatorTests {
    private static Logger log = LoggerFactory.getLogger(OrderedParallelOperatorTests.class);

    @Test
    public void TestGetNextIndexToEmit() throws Exception {
        // parallelism 2
        assertEquals(1, OrderedParallelOperator.getNextIndexToEmit(0, 0, 2));
        assertEquals(2, OrderedParallelOperator.getNextIndexToEmit(1, 0, 2));

        // parallelism 3
        assertEquals(1, OrderedParallelOperator.getNextIndexToEmit(0, 0, 3));
        assertEquals(3, OrderedParallelOperator.getNextIndexToEmit(1, 0, 3));
        assertEquals(4, OrderedParallelOperator.getNextIndexToEmit(3, 0, 3));
        assertEquals(6, OrderedParallelOperator.getNextIndexToEmit(4, 0, 3));
        assertEquals(1, OrderedParallelOperator.getNextIndexToEmit(0, 1, 3));
        assertEquals(2, OrderedParallelOperator.getNextIndexToEmit(1, 1, 3));
        assertEquals(3, OrderedParallelOperator.getNextIndexToEmit(2, 1, 3));

        // parallelism 4
        assertEquals(1, OrderedParallelOperator.getNextIndexToEmit(0, 0, 4));
        assertEquals(4, OrderedParallelOperator.getNextIndexToEmit(1, 0, 4));
        assertEquals(5, OrderedParallelOperator.getNextIndexToEmit(4, 0, 4));
        assertEquals(1, OrderedParallelOperator.getNextIndexToEmit(0, 1, 4));
        assertEquals(2, OrderedParallelOperator.getNextIndexToEmit(1, 1, 4));
        assertEquals(4, OrderedParallelOperator.getNextIndexToEmit(2, 1, 4));
        assertEquals(1, OrderedParallelOperator.getNextIndexToEmit(0, 2, 4));
        assertEquals(2, OrderedParallelOperator.getNextIndexToEmit(1, 2, 4));
        assertEquals(3, OrderedParallelOperator.getNextIndexToEmit(2, 2, 4));
        assertEquals(4, OrderedParallelOperator.getNextIndexToEmit(3, 2, 4));
        assertEquals(5, OrderedParallelOperator.getNextIndexToEmit(4, 2, 4));
    }
}
