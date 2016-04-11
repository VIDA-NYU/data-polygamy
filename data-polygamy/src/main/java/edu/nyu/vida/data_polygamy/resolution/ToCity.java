/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.resolution;

import java.util.ArrayList;

public class ToCity implements SpatialResolution {
    
	private int[] positions;
	
    public ToCity(int[] spatialPos) {
    	this.positions = spatialPos;
    }

    @Override
    public ArrayList<Integer> translate(String[] input) {
        // assuming all the neighborhood and grid data is from NYC
        ArrayList<Integer> output = new ArrayList<Integer>();
        for (int i = 0; i < positions.length; i++)
        	output.add(0);
        return output;
    }
    
    @Override
    public int translate(int[] input) {
        // assuming all the neighborhood and grid data is from NYC
        return 0;
    }

}
