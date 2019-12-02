/*
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.example.tensorflow;

/**
 * ArgMax function to select the higher value and its index from the array.
 *
 * Created by Zoltan Szabo on 1/5/18.
 * URL: https://github.com/szaza/android-yolo-v2
 */
public class ArgMax {

    private double[] params;

    public ArgMax(double[] params) {
        this.params = params;
    }

    public Result getResult() {
        int maxIndex = 0;
        for (int i=0; i<params.length; i++) {
            if (params[maxIndex] < params[i]) {
                maxIndex = i;
            }
        }

        return new Result(maxIndex, params[maxIndex]);
    }

    public class Result {
        private int index;
        private double maxValue;

        public Result(int index, double maxValue) {
            this.index = index;
            this.maxValue = maxValue;
        }

        public int getIndex() {
            return index;
        }

        public double getMaxValue() {
            return maxValue;
        }
    }
}
