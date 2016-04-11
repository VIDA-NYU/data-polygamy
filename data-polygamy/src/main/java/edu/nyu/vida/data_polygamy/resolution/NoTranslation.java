/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.resolution;

import java.util.ArrayList;

public class NoTranslation implements SpatialResolution {
    
    private int[] spatialPos;
    
    public NoTranslation(int[] spatialPos) {
        this.spatialPos = spatialPos;
    }

    @Override
    public ArrayList<Integer> translate(String[] input) {
        ArrayList<Integer> output = new ArrayList<Integer>();
        try {
            for (int pos = 0; pos < spatialPos.length; pos++)
                output.add(Integer.parseInt(input[spatialPos[pos]]));
        } catch (NumberFormatException e) {
            System.out.println("Something is wrong...");
            e.printStackTrace();
            System.exit(1);
        }
        return output;
    }
    
    @Override
    public int translate(int[] input) {
        int output = 0;
        try {
            output = input[spatialPos[0]];
        } catch (NumberFormatException e) {
            System.out.println("Something is wrong...");
            output = -1;
        }
        return output;
    }

}
