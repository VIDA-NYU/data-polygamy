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

public class BlockToBlock implements SpatialResolution {
    
    private int[] positions;
    private HashMap<Integer, Integer> blockMap = new HashMap<Integer, Integer>();
    
    private String data;
    
    public BlockToBlock(int[] spatialPos, Configuration conf) {
        this.positions = spatialPos;
        
        String bucket = conf.get("bucket", "");
        
        data = bucket + "block";
        
        try {
            if (bucket.equals("")) {
                FileSystem fs = FileSystem.get(new Configuration());
                readData(fs.open(new Path(data)));
            } else {
            	Path dataPath = new Path(data);
                FileSystem fs = FileSystem.get(dataPath.toUri(), conf);
                readData(fs.open(dataPath));
                fs.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
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
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public ArrayList<Integer> translate(String[] input)
            throws NumberFormatException, IllegalArgumentException, NullPointerException {
        
        ArrayList<Integer> region = new ArrayList<Integer>();
        
        // getting spatial attributes
        int block = 0;
        boolean foundOne = false;
        for (int i = 0; i < positions.length; i++) {
            
            if (input[positions[i]] == null)
                continue;
          
            try {
                block = (int)Double.parseDouble(input[positions[i]].trim());
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
