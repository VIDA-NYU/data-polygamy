/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.resolution;

import org.apache.hadoop.conf.Configuration;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;

public class SpatialResolutionUtils {

    public static SpatialResolution pointsResolution(int spatialResolution,
            int gridResolution, int[] xPositions, int[] yPositions,
            Configuration conf) {
        
        SpatialResolution spatialTranslation = null;
        
        switch (spatialResolution) {
        
        case FrameworkUtils.NBHD:
            spatialTranslation = new PointsToRegion(xPositions, yPositions, "nbhd",
                    gridResolution, conf);
            break;
        case FrameworkUtils.ZIP:
            spatialTranslation = new PointsToRegion(xPositions, yPositions, "zip",
                    gridResolution, conf);
            break;
        case FrameworkUtils.GRID:
            spatialTranslation = new PointsToRegion(xPositions, yPositions, "grid",
                    gridResolution, conf);
            break;
        case FrameworkUtils.BLOCK:
            spatialTranslation = new PointsToRegion(xPositions, yPositions, "block",
                    gridResolution, conf);
            break;
        case FrameworkUtils.CITY:
            spatialTranslation = new ToCity(xPositions);
            break;
        default:
            System.out.println("Something is wrong...");
            System.exit(-1);
            break;
        }
        
        return spatialTranslation;
    }
    
    public static SpatialResolution bblResolution(int spatialResolution,
            int[] spatialPos, Configuration conf) {
        
        SpatialResolution spatialTranslation = null;
        
        switch (spatialResolution) {
        
        case FrameworkUtils.NBHD:
            spatialTranslation = new BblToNbhd(spatialPos, conf);
            break;
        case FrameworkUtils.ZIP:
            System.out.println("Bbl to Zip currently not supported.");
            System.exit(-1);
            break;
        case FrameworkUtils.GRID:
            System.out.println("Bbl to Grid currently not supported.");
            System.exit(-1);
            break;
        case FrameworkUtils.BLOCK:
            spatialTranslation = new BblToBlock(spatialPos, conf);
            break;
        case FrameworkUtils.CITY:
            spatialTranslation = new ToCity(spatialPos);
            break;
        default:
            System.out.println("Something is wrong...");
            System.exit(-1);
            break;
        }
        
        return spatialTranslation;
    }
    
    public static SpatialResolution blockResolution(int spatialResolution,
            int[] spatialPos, boolean preProcessing, Configuration conf) {
        
        SpatialResolution spatialTranslation = null;
        
        switch (spatialResolution) {
        
        case FrameworkUtils.NBHD:
            spatialTranslation = new BlockToNbhd(spatialPos, conf);
            break;
        case FrameworkUtils.BLOCK:
            if (preProcessing)
                spatialTranslation = new BlockToBlock(spatialPos, conf);
            else
                spatialTranslation = new NoTranslation(spatialPos);
            break;
        case FrameworkUtils.GRID:
            System.out.println("Block to Grid currently not supported.");
            System.exit(-1);
            break;
        case FrameworkUtils.ZIP:
            System.out.println("Block to Zip currently not supported.");
            System.exit(-1);
            break;
        case FrameworkUtils.CITY:
            spatialTranslation = new ToCity(spatialPos);
            break;
        default:
            System.out.println("Something is wrong...");
            System.exit(-1);
            break;
        }
        
        return spatialTranslation;
    }
    
    public static SpatialResolution nbhdResolution(int spatialResolution,
            int[] spatialPos) {
        
        SpatialResolution spatialTranslation = null;
        
        switch (spatialResolution) {
        
        case FrameworkUtils.NBHD:
            spatialTranslation = new NoTranslation(spatialPos);
            break;
        case FrameworkUtils.ZIP:
        	System.out.println("Nbhd to Zip currently not supported.");
            System.exit(-1);
        	break;
        case FrameworkUtils.GRID:
        	System.out.println("Nbhd to Grid currently not supported.");
            System.exit(-1);
        	break;
        case FrameworkUtils.BLOCK:
            System.out.println("Nbhd to Block currently not supported.");
            System.exit(-1);
            break;
        case FrameworkUtils.CITY:
            spatialTranslation = new ToCity(spatialPos);
            break;
        default:
            System.out.println("Something is wrong...");
            System.exit(-1);
            break;
        }
        
        return spatialTranslation;
    }
    
    public static SpatialResolution zipResolution(int spatialResolution,
            int[] spatialPos, boolean preProcessing, Configuration conf) {
        
        SpatialResolution spatialTranslation = null;
        
        switch (spatialResolution) {
        
        case FrameworkUtils.NBHD:
        	System.out.println("Zip to Nbhd currently not supported.");
            System.exit(-1);
            break;
        case FrameworkUtils.ZIP:
            if (preProcessing)
                spatialTranslation = new ZipToZip(spatialPos, conf);
            else
                spatialTranslation = new NoTranslation(spatialPos);
            break;
        case FrameworkUtils.GRID:
        	System.out.println("Zip to Grid currently not supported.");
            System.exit(-1);
            break;
        case FrameworkUtils.BLOCK:
            System.out.println("Zip to Block currently not supported.");
            System.exit(-1);
            break;
        case FrameworkUtils.CITY:
            spatialTranslation = new ToCity(spatialPos);
            break;
        default:
            System.out.println("Something is wrong...");
            System.exit(-1);
            break;
        }
        
        return spatialTranslation;
    }
    
    public static SpatialResolution gridResolution(int spatialResolution,
            int[] spatialPos) {
        
        SpatialResolution spatialTranslation = null;
        
        switch (spatialResolution) {
        
        case FrameworkUtils.NBHD:
        	System.out.println("Grid to Nbhd currently not supported.");
            System.exit(-1);
            break;
        case FrameworkUtils.ZIP:
        	System.out.println("Grid to Zip currently not supported.");
            System.exit(-1);
        	break;
        case FrameworkUtils.GRID:
            spatialTranslation = new NoTranslation(spatialPos);
            break;
        case FrameworkUtils.BLOCK:
            System.out.println("Grid to Block currently not supported.");
            System.exit(-1);
            break;
        case FrameworkUtils.CITY:
            spatialTranslation = new ToCity(spatialPos);
            break;
        default:
            System.out.println("Something is wrong...");
            System.exit(-1);
            break;
        }
        
        return spatialTranslation;
    }
    
}
