/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.resolution;

import java.awt.geom.Path2D;
import java.awt.geom.Rectangle2D;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BblToNbhd implements SpatialResolution {
    
    private int[] spatialPos;
    
    private String dataBbl;
    private String dataNbhd;
    
    private ArrayList<Integer> nbhdRegionNames = new ArrayList<Integer>();
    private HashMap<Long, Integer> bblRegions =
            new HashMap<Long, Integer>();
    private GridIndex grid = new GridIndex(100, 100);
    
    public BblToNbhd(int[] spatialPos, Configuration conf) {
        
    	String bucket = conf.get("bucket", "");
    	dataBbl = bucket + "bbl";
        dataNbhd = bucket + "neighborhood";
        
        this.spatialPos = spatialPos;
        
        try {
            if (bucket.equals("")) {
                FileSystem fs = FileSystem.get(new Configuration());
                readNbhdData(fs.open(new Path(dataNbhd)));
                readBblData(fs.open(new Path(dataBbl)));
            } else {
                Path nbhdPath = new Path(dataNbhd);
                FileSystem fs = FileSystem.get(nbhdPath.toUri(), conf);
                readNbhdData(fs.open(nbhdPath));
                Path bblPath = new Path(dataBbl);
                fs = FileSystem.get(bblPath.toUri(), conf);
                readBblData(fs.open(bblPath));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
  //@SuppressWarnings("unchecked")
    private void readNbhdData(FSDataInputStream fis) throws IOException {
        
        ArrayList<Path2D.Double> allPolygons = new ArrayList<Path2D.Double>();
        
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
                nbhdRegionNames.add(Integer.parseInt(region));
                
                line = buff.readLine();
            }
            
            buff.close();
            
            grid.buildGrid(allPolygons, false);
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            fis.close();
        }
    }
    
    private void readBblData(FSDataInputStream fis) throws IOException {
        
        try {
            BufferedReader buff = new BufferedReader(new InputStreamReader(fis));
            String line = buff.readLine();
            
            ArrayList<Double> xPoints = new ArrayList<Double>();
            ArrayList<Double> yPoints = new ArrayList<Double>();
            
            while (line != null) {
                String region = line.trim();
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
                
                Rectangle2D rect = polygon.getBounds2D();
                double x = rect.getCenterX();
                double y = rect.getCenterY();
                
                int r = grid.getRegion(x, y);
                if(r != -1) {
                    bblRegions.put(Long.parseLong(region), nbhdRegionNames.get(r));
                }
                
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
        long bbl = 0;
        boolean foundOne = false;
        for (int i = 0; i < spatialPos.length; i++) {
          
            try {
                bbl = (long)Double.parseDouble(input[spatialPos[i]]);
            } catch (NumberFormatException e) {
                // no information regarding coordinates
                continue;
            }
            
            Integer nbhd = bblRegions.get(bbl);
            if (nbhd == null) {
                region.add(-1);
            } else {
                region.add(nbhd);
                foundOne = true;
            }
                
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
