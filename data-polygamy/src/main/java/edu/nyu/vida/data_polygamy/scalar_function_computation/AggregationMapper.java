/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.scalar_function_computation;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.nyu.vida.data_polygamy.resolution.SpatialResolution;
import edu.nyu.vida.data_polygamy.resolution.SpatialResolutionUtils;
import edu.nyu.vida.data_polygamy.resolution.ToCity;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AggregationArrayWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.MultipleSpatioTemporalWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalWritable;

public class AggregationMapper extends Mapper<MultipleSpatioTemporalWritable, AggregationArrayWritable, SpatioTemporalWritable, AggregationArrayWritable> {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    
    HashMap<String,String> datasetToId = new HashMap<String,String>();
    
    int spatialIndex, tempIndex;
    int currentTemporal, currentSpatial;
    int[] temporalResolutions, spatialResolutions;
    int invalidSpatial = -1;
    int invalidTemporal = -1;
    int datasetId = -1;
    boolean multiplePreProcessing = false;
    
    int temporalResolution, spatialResolution;
    boolean sameResolution = false;
    SpatialResolution[] spatialTranslation;
    
    // output key
    SpatioTemporalWritable keyWritable = new SpatioTemporalWritable();
    
    private SpatialResolution resolveResolution(int currentSpatialResolution,
            int spatialResolution, int spatialPos, Configuration conf) {
        
        SpatialResolution spatialTranslation = null;
        int[] spatialPosArray = new int[1];
        spatialPosArray[0] = spatialPos;
        
        switch (currentSpatialResolution) {
        
        case FrameworkUtils.NBHD:
            spatialTranslation = SpatialResolutionUtils.nbhdResolution(spatialResolution,
            		spatialPosArray);
            break;
        case FrameworkUtils.ZIP:
            spatialTranslation = SpatialResolutionUtils.zipResolution(spatialResolution,
            		spatialPosArray, false, conf);
            break;
        case FrameworkUtils.GRID:
            spatialTranslation = SpatialResolutionUtils.gridResolution(spatialResolution,
            		spatialPosArray);
            break;
        case FrameworkUtils.BLOCK:
            spatialTranslation = SpatialResolutionUtils.blockResolution(spatialResolution,
                    spatialPosArray, false, conf);
            break;
        case FrameworkUtils.CITY:
            spatialTranslation = new ToCity(spatialPosArray);
            break;
        default:
            System.out.println("Something is wrong...");
            System.exit(-1);
        }
        
        return spatialTranslation;
    }

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
    
        Configuration conf = context.getConfiguration();
        
        String[] datasetNames = conf.get("dataset-name","").split(",");
        String[] datasetIds = conf.get("dataset-id","").split(",");
        for (int i = 0; i < datasetNames.length; i++) 
            datasetToId.put(datasetNames[i], datasetIds[i]);
        
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String[] fileSplitTokens = fileSplit.getPath().getParent().toString().split("/");
        String[] filenameTokens = fileSplitTokens[fileSplitTokens.length-1].split("-");
        String dataset = "";
        for (int i = 0; i < filenameTokens.length-2; i++) {
            dataset += filenameTokens[i] + "-";
        }
        dataset = dataset.substring(0, dataset.length()-1);
        String datasetIdStr = datasetToId.get(dataset);
        
        currentTemporal = utils.temporalResolution(
                filenameTokens[filenameTokens.length-2]);
        currentSpatial = utils.spatialResolution(
                filenameTokens[filenameTokens.length-1]);
        
        datasetId = Integer.parseInt(datasetIdStr);
        
        try {
            tempIndex = Integer.parseInt(conf.get("dataset-" + datasetIdStr +
                    "-temporal-att","0"));
            spatialIndex = Integer.parseInt(conf.get("dataset-" + datasetIdStr +
                    "-spatial-att", "0"));
            multiplePreProcessing = Boolean.parseBoolean(
                    conf.get("dataset-" + datasetIdStr + "-multiple", "false"));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        
        String[] temporalResolutionArray =
                FrameworkUtils.getAggTempResolutions(currentTemporal);
        String[] spatialResolutionArray = new String[1];
        
        // if data has more than one spatial res., choose nbhd to translate to city
        if (multiplePreProcessing && (currentSpatial != FrameworkUtils.NBHD)) {
            spatialResolutionArray[0] =
                    FrameworkUtils.getAggSpatialResolutions(currentSpatial)[0];
        } else {
            spatialResolutionArray =
                    FrameworkUtils.getAggSpatialResolutions(currentSpatial);
        }
        
        int size = temporalResolutionArray.length * spatialResolutionArray.length;
        
        temporalResolutions = new int[size];
        spatialResolutions = new int[size];
        
        int id = 0;
        for (int i = 0; i < temporalResolutionArray.length; i++) {
            for (int j = 0; j < spatialResolutionArray.length; j++) {
                temporalResolutions[id] = utils.temporalResolution(temporalResolutionArray[i]);
                spatialResolutions[id] = utils.spatialResolution(spatialResolutionArray[j]);
                id++;
            }
        }
        
        /**
         * Spatial Translations
         */
        
        spatialTranslation = new SpatialResolution[spatialResolutions.length];
        for (int i = 0; i < spatialResolutions.length; i++) {
            spatialTranslation[i] = resolveResolution(
                    currentSpatial, spatialResolutions[i], spatialIndex,
                    context.getConfiguration());
        }
    }
    
    @Override
    public void map(MultipleSpatioTemporalWritable key, AggregationArrayWritable value, Context context)
            throws IOException, InterruptedException {
        
        for (int i = 0; i < temporalResolutions.length; i++) {
            
            // input
            int[] spatialArray = key.getSpatial();
            int[] temporalArray = key.getTemporal();
            
            int spatialAtt = -1;
            int temporalAtt = -1;
            
    		temporalResolution = temporalResolutions[i];
    		spatialResolution = spatialResolutions[i];
    		
    		sameResolution = (temporalResolution == currentTemporal) &&
    				(spatialResolution == currentSpatial);
    		
    		if (sameResolution) {
                spatialAtt = spatialArray[spatialIndex];
                temporalAtt = temporalArray[tempIndex];
                if ((spatialAtt != invalidSpatial) && (temporalAtt != invalidTemporal)) {
                    keyWritable = new SpatioTemporalWritable(spatialAtt,
                            temporalAtt, spatialResolution, temporalResolution,
                            datasetId);
                    context.write(keyWritable, value);
                }
                continue;
            }
            
            /**
             * Spatial Resolution
             */
            
            spatialAtt = spatialArray[spatialIndex];
            
            if (currentSpatial != spatialResolution)
                spatialAtt = spatialTranslation[i].translate(spatialArray);
            
            if (spatialAtt == invalidSpatial)
                continue;
            
            /**
             * Temporal Resolution
             */
            
            if (currentTemporal == temporalResolution)
                temporalAtt = temporalArray[tempIndex];
            else
                temporalAtt = FrameworkUtils.getTime(temporalResolution, temporalArray, tempIndex);
            
            if (temporalAtt < 0)
                continue;
            
            keyWritable = new SpatioTemporalWritable(spatialAtt, temporalAtt,
            		spatialResolution, temporalResolution, datasetId);
            context.write(keyWritable, value);
        }
    }
    
}
