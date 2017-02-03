/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.resolution;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BblToBlock implements SpatialResolution {
    
    private int[] spatialPos;
    
    private String data;
    
    private HashMap<Integer, Integer> blockMap = new HashMap<Integer, Integer>();
    
    public BblToBlock(int[] spatialPos, Configuration conf) {
        
        String bucket = conf.get("bucket", "");
    	data = bucket + "block";
    	
        this.spatialPos = spatialPos;
        
        try {
            if (bucket.equals("")) {
                FileSystem fs = FileSystem.get(new Configuration());
                readData(fs.open(new Path(data)));
            } else {
            	Path dataPath = new Path(data);
                FileSystem fs = FileSystem.get(dataPath.toUri(), conf);
                readData(fs.open(dataPath));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    //@SuppressWarnings("unchecked")
    private void readData(FSDataInputStream fis) throws IOException {
        
        int id = 0;
        
        try {
            BufferedReader buff = new BufferedReader(new InputStreamReader(fis));
            String line = buff.readLine();
            
            while (line != null) {
                String region = line;
                buff.readLine();
                Integer nbPoints = Integer.parseInt(buff.readLine());
                
                for (int i = 0; i < nbPoints; i++)
                    buff.readLine();
                
                blockMap.put(Integer.parseInt(region), id);
                id++;
                
                line = buff.readLine();
            }
            
            buff.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            fis.close();
        }
        
    }

    @Override
    public ArrayList<Integer> translate(String[] input) {
        
        ArrayList<Integer> region = new ArrayList<Integer>();
        
        // getting spatial attributes
        int block = 0;
        boolean foundOne = false;
        for (int i = 0; i < spatialPos.length; i++) {
            
            if (input[spatialPos[i]] == null)
                continue;
          
            String bbl = input[spatialPos[i]].trim();
            if (bbl.length() != 10)
                continue;
            
            try {
                // removing the last 4 digits (lot information)
                block = (int)Double.parseDouble(bbl.substring(0, bbl.length() - 4));
            } catch (NumberFormatException e) {
                // no information regarding block
                continue;
            }
          
            if(blockMap.containsKey(block)) {
                region.add(blockMap.get(block));
                foundOne = true;
            }
            else
                region.add(-1);
        }
      
        if (foundOne)
            return region;
        return new ArrayList<Integer>();
    }
    
    @Override
    public int translate(int[] input) {
        // should not be called
        return -1;
    }

}
