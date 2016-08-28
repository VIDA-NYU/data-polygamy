/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.exp;

import java.io.IOException;

import edu.nyu.vida.data_polygamy.exp.CorrelationGraph;
import edu.nyu.vida.data_polygamy.exp.RelationGraph;

public class DataRelationships {

	public static void main(String[] args) {
	    
	    String type = args[0];
	    if (type.equals("relationship")) {
    		RelationGraph g = new RelationGraph();
    		
    		String dataPath = args[1];
    		String metadataPath = args[2];
    		String events = args[3];
    		String perm = args[4];
    		String score = args[5];
    		String strength = args[6];
    		String pValue = args[7];
    		String temporalRes = args[8];
    		String spatialRes = args[9];
    		String outputDir = args[10];
    		
    		try {
    			g.createGraph(
    			        dataPath,
    			        metadataPath,
    			        events,
    			        perm,
    			        temporalRes,
    			        spatialRes);
    			
    			g.printMetaGraph(outputDir,
    			        score,
                        strength,
                        pValue);
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
	    } else if (type.equals("correlation")) {
            CorrelationGraph g = new CorrelationGraph();
            
            String dataPath = args[1];
            String metadataPath = args[2];
            String pearsonCorrTh = args[3];
            String miTh = args[4];
            String dtwTh = args[5];
            String temporalRes = args[6];
            String spatialRes = args[7];
            String outputDir = args[8];
            
            try {
                g.createGraph(
                        dataPath,
                        metadataPath,
                        temporalRes,
                        spatialRes);
                
                g.printMetaGraph(outputDir,
                        pearsonCorrTh,
                        CorrelationGraph.PEARSON);
                
                g.printMetaGraph(outputDir,
                        miTh,
                        CorrelationGraph.MUTUAL);
                
                g.printMetaGraph(outputDir,
                        dtwTh,
                        CorrelationGraph.DTW);
            } catch (IOException e) {
                e.printStackTrace();
            }
	    } else {
	        System.out.println("Invalid option");
	        System.exit(0);
	    }
	}

}
