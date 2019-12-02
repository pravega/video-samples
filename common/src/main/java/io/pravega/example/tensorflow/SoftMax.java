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

public class SoftMax {
    private final double[] params;

    public SoftMax(double[] params) {
        this.params = params;
    }

    public double[] getValue() {
        double sum = 0;

        for (int i=0; i<params.length; i++) {
            params[i] = Math.exp(params[i]);
            sum += params[i];
        }

        if (Double.isNaN(sum) || sum < 0) {
            for (int i=0; i<params.length; i++) {
                params[i] = 1.0 / params.length;
            }
        } else {
            for (int i=0; i<params.length; i++) {
                params[i] = params[i] / sum;
            }
        }

        return params;
    }
}
