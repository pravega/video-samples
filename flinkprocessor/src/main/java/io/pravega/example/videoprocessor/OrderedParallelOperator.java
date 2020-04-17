package io.pravega.example.videoprocessor;

import com.google.common.base.Preconditions;

public class OrderedParallelOperator {
    /**
     * Operator 0 expects to receive streams 0 to 1.
     * Operator 1 expects to receive streams 0 to 2.
     * @param currentIndex The first index for all streams is 0.
     * @param operator The number of this operator (0 to parallelism-2).
     * @param parallelism The number of streams.
     * @return The index of the next element that this operator will emit.
     */
    static long getNextIndexToEmit(final long currentIndex, final int operator, final int parallelism) {
        Preconditions.checkArgument(0 <= currentIndex);
        Preconditions.checkArgument(0 <= operator);
        Preconditions.checkArgument( 2 <= parallelism);
        Preconditions.checkArgument(operator < parallelism - 1);
        Preconditions.checkArgument(currentIndex % parallelism <= operator + 1);
        long index = currentIndex + 1;
        final long stream = index % parallelism;
        if (stream <= operator + 1) {
            return index;
        } else {
            return (index / parallelism + 1) * parallelism;
        }
    }
}