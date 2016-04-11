/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.resolution;

import java.util.ArrayList;

public interface SpatialResolution {

    ArrayList<Integer> translate(String[] input);
    int translate(int[] input);
    
}
