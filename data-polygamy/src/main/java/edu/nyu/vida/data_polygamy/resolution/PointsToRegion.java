/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.resolution;

import java.awt.geom.Path2D;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PointsToRegion implements SpatialResolution {
    
    private int[] xPositions;
    private int[] yPositions;
    
    private String data;
    
    private ArrayList<Integer> polyRegionNames = new ArrayList<Integer>();
    private GridIndex grid = new GridIndex(100, 100);
    boolean zipcode = false;
    
    public PointsToRegion(int[] xPositions, int[] yPositions, String region,
            int gridResolution, Configuration conf) {
        
    	String bucket = conf.get("bucket", "");
    	
        if (region.equals("nbhd"))
            data = bucket + "neighborhood";
        else if (region.equals("grid"))
            data = bucket + "gneighborhood-" + gridResolution;
        else if (region.equals("zip")) {
        	zipcode = true;
        	data = bucket + "zipcode";
        } else {
        	System.out.println("Invalid region.");
        	System.exit(-1);
        }
        
        this.xPositions = xPositions;
        this.yPositions = yPositions;
        
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
    
    //@SuppressWarnings("unchecked")
    private void readData(FSDataInputStream fis) throws IOException {
        
        ArrayList<Path2D.Double> allPolygons = new ArrayList<Path2D.Double>();
        /*Set<String> regionNames;
        JSONParser parser = new JSONParser();*/
        
        int zipcodeId = 0;
        
        try {
            BufferedReader buff = new BufferedReader(new InputStreamReader(fis));
            String line = buff.readLine();
            
            ArrayList<Double> xPoints = new ArrayList<Double>();
            ArrayList<Double> yPoints = new ArrayList<Double>();
            
            while (line != null) {
                String region = line;
                buff.readLine();
                Integer nbPoints = Integer.parseInt(buff.readLine());
                
                xPoints = new ArrayList<Double>(nbPoints);
                yPoints = new ArrayList<Double>(nbPoints);
                for (int i = 0; i < nbPoints; i++) {
                    String[] points = buff.readLine().split(" ");
                    xPoints.add(Double.parseDouble(points[0]));
                    yPoints.add(Double.parseDouble(points[1]));
                }
                
                // creating polygon
                Path2D polygon = new Path2D.Double();
                polygon.moveTo(xPoints.get(0), yPoints.get(0));
                for (int i = 1; i < xPoints.size(); ++i) {
                    polygon.lineTo(xPoints.get(i), yPoints.get(i));
                }
                polygon.closePath();
              
                allPolygons.add((Path2D.Double) polygon);
                if (zipcode) {
                    polyRegionNames.add(zipcodeId);
                    zipcodeId++;
                } else
                    polyRegionNames.add(Integer.parseInt(region));
                
                line = buff.readLine();
            }
            
            grid.buildGrid(allPolygons);
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public ArrayList<Integer> translate(String[] input) {
        
        ArrayList<Integer> region = new ArrayList<Integer>();
      
        // getting spatial attributes
        double x = 0, y = 0;
        boolean foundOne = false;
        for (int i = 0; i < xPositions.length; i++) {
          
            try {
                x = Double.parseDouble(input[xPositions[i]]);
                y = Double.parseDouble(input[yPositions[i]]);
            } catch (NumberFormatException e) {
                // no information regarding coordinates
                continue;
            }
          
            int r = grid.getRegion(x, y);
            if(r != -1) {
                region.add(polyRegionNames.get(r));
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
