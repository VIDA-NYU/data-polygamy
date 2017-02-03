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

public class ZipToZip implements SpatialResolution {
    
    private int[] positions;
    private HashMap<Integer, Integer> zipcodeMap = new HashMap<Integer, Integer>();
    
    private String data;
    
    public ZipToZip(int[] spatialPos, Configuration conf) {
        this.positions = spatialPos;
        
        String bucket = conf.get("bucket", "");
        
        data = bucket + "zipcode";
        
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
    
    private void readData(FSDataInputStream fis) throws IOException {
        
        int zipcodeId = 0;
        
        try {
        	BufferedReader buff = new BufferedReader(new InputStreamReader(fis));
            String line = buff.readLine();
            
            while (line != null) {
                String region = line;
                buff.readLine();
                Integer nbPoints = Integer.parseInt(buff.readLine());
                
                for (int i = 0; i < nbPoints; i++)
                    buff.readLine();
                
                zipcodeMap.put(Integer.parseInt(region), zipcodeId);
                zipcodeId++;
                
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
    public ArrayList<Integer> translate(String[] input)
            throws NumberFormatException, IllegalArgumentException, NullPointerException {
        
        ArrayList<Integer> region = new ArrayList<Integer>();
        
        // getting spatial attributes
        int zip = 0;
        boolean foundOne = false;
        for (int i = 0; i < positions.length; i++) {
            
            if (input[positions[i]] == null)
                continue;
          
            try {
                zip = (int)Double.parseDouble(input[positions[i]].trim());
            } catch (NumberFormatException e) {
            	// no information regarding zipcode
                continue;
            }
          
            if(zipcodeMap.containsKey(zip)) {
                region.add(zipcodeMap.get(zip));
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
