/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.pre_processing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.nyu.vida.data_polygamy.scalar_function.Aggregation;
import edu.nyu.vida.data_polygamy.scalar_function.Count;
import edu.nyu.vida.data_polygamy.scalar_function.CountGradient;
import edu.nyu.vida.data_polygamy.resolution.SpatialResolution;
import edu.nyu.vida.data_polygamy.resolution.SpatialResolutionUtils;
import edu.nyu.vida.data_polygamy.resolution.ToCity;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AggregationArrayWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Function;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.MultipleSpatioTemporalWritable;

/**
 * 
 * @author fchirigati
 *
 */
public class PreProcessingMapper extends Mapper<LongWritable, Text, MultipleSpatioTemporalWritable, AggregationArrayWritable> {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    Configuration conf = null;
    long records = 0L;
    boolean s3 = true;
    
    // output
    MultipleSpatioTemporalWritable keyWritable = new MultipleSpatioTemporalWritable();
    AggregationArrayWritable valueWritable = new AggregationArrayWritable();
    
    // input parameters
    int temporalResolution, spatialResolution, gridResolution, currentSpatialResolution;
    int[] xPositions, yPositions, spatialPos, temporalPos;
    
    // parameter names
    String[] parameterNames, keyNames, paramDefaults;
    
    // aggregation functions for each parameter
    boolean aggregatesInit = false;
    ArrayList<Integer> aggregatesIndex = new ArrayList<Integer>();
    HashMap<Integer,Integer> attributeIndex =
            new HashMap<Integer,Integer>();
    HashMap<Integer,String> aggregates =
            new HashMap<Integer,String>();
    HashMap<Integer,Function> aggregateFunctions =
            new HashMap<Integer,Function>();
    
    SpatialResolution spatialTranslation = null;
    int sizeSpatioTemp = 0;
    int nbParameters = 1;
    
    private void identifyAggregates(String[] input) {
        
        String[] inputTest = Arrays.copyOf(input, input.length);
        
        // skip spatio-temporal attributes
        // assuming reading first spatial than temporal for the headers
        keyNames = new String[xPositions.length +
                              spatialPos.length +
                              temporalPos.length];
        int keyNamesIndex = 0;
        if (xPositions.length == 0)
            for (int i = 0; i < spatialPos.length; i++) {
                inputTest[spatialPos[i]] = null;
                keyNames[keyNamesIndex++] = parameterNames[spatialPos[i]].trim();
            }
        else {
            for (int i = 0; i < xPositions.length; i++) {
                inputTest[xPositions[i]] = null;
                inputTest[yPositions[i]] = null;
                keyNames[keyNamesIndex++] = parameterNames[xPositions[i]].trim();
            }
        }
        
        for (int i = 0; i < temporalPos.length; i++) {
            inputTest[temporalPos[i]] = null;
            keyNames[keyNamesIndex++] = parameterNames[temporalPos[i]].trim();
        }
        
        /*System.out.println("Identifying attributes...");
        for (int i = 0; i < keyNames.length; i++)
        	System.out.print(keyNames[i] + ",");
        System.out.println();*/
        
        // count
        aggregates.put((nbParameters-1),
                (nbParameters-1) + "-" + FrameworkUtils.functionToString(Function.COUNT)
                + "-" + parameterNames[0].trim());
        attributeIndex.put((nbParameters-1), -1);
        aggregateFunctions.put((nbParameters-1), Function.COUNT);
        
        nbParameters++;
        
        // count gradient
        aggregates.put((nbParameters-1),
                (nbParameters-1) + "-" + FrameworkUtils.functionToString(Function.COUNT_GRADIENT)
                + "-" + parameterNames[0].trim());
        attributeIndex.put((nbParameters-1), -2);
        aggregateFunctions.put((nbParameters-1), Function.COUNT_GRADIENT);
        
        for (int i = 0; i < inputTest.length; i++) {
            
            if (inputTest[i] == null)
                continue;
            
            if (inputTest[i].startsWith("$") && inputTest[i].endsWith("$"))
                continue;
            
            if (inputTest[i].startsWith("\"") && inputTest[i].endsWith("\""))
                continue;
            
            /*if (!FrameworkUtils.isNumeric(inputTest[i]))
                continue;*/
            
            nbParameters++;
            
            // id fields -- only use unique aggregate
            String parameterNameLowerCase = parameterNames[i].toLowerCase(); 
            if (parameterNameLowerCase.contains("id") || 
                    parameterNameLowerCase.contains("key") ||
                    parameterNameLowerCase.contains("name")) {
                
                aggregates.put((nbParameters-1),
                        (nbParameters-1) + "-" + FrameworkUtils.functionToString(Function.UNIQUE)
                        + "-" + parameterNames[i].trim());
                attributeIndex.put((nbParameters-1), i);
                aggregateFunctions.put((nbParameters-1), Function.UNIQUE);
                
                continue;
            }
            
            aggregates.put((nbParameters-1),
                    (nbParameters-1) + "-" + FrameworkUtils.functionToString(Function.AVERAGE)
                    + "-" + parameterNames[i].trim());
            attributeIndex.put((nbParameters-1), i);
            aggregateFunctions.put((nbParameters-1), Function.AVERAGE);
            
            nbParameters++;
            
            aggregates.put((nbParameters-1),
                    (nbParameters-1) + "-" + FrameworkUtils.functionToString(Function.GRADIENT)
                    + "-" + parameterNames[i].trim());
            attributeIndex.put((nbParameters-1), i);
            aggregateFunctions.put((nbParameters-1), Function.GRADIENT);
            
        }
        
        Iterator<Integer> it = attributeIndex.keySet().iterator();
        while (it.hasNext())
            aggregatesIndex.add(it.next());
    }
  
    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
      
        conf = context.getConfiguration();
        
        String bucket = conf.get("bucket", "");
        if (bucket.equals(""))
            s3 = false;
        
        // defaults
        Path defaults = new Path(conf.get("defaults", "")); 
        
        FileSystem fs = null;
        BufferedReader br = null;
        
        if (!s3)
            fs = FileSystem.get(new Configuration());
        else
            fs = FileSystem.get(defaults.toUri(), conf);
        br = new BufferedReader(new InputStreamReader(fs.open(defaults)));
        paramDefaults = br.readLine().split(",");
        br.close();
        if (s3)
        	fs.close();
        
        temporalResolution = utils.temporalResolution(conf.get("temporal-resolution"));
        spatialResolution = utils.spatialResolution(conf.get("spatial-resolution"));
        currentSpatialResolution = utils.spatialResolution(conf.get("current-spatial-resolution"));
        gridResolution = ( conf.get("grid-resolution","").equals("") ) ? 0 : Integer.parseInt(conf.get("grid-resolution",""));
        sizeSpatioTemp = Integer.parseInt(conf.get("size-spatio-temporal", "0"));
        
        // positions
        String[] temporalArray = ( conf.get("temporal-pos","").equals("") ) ? new String[0] : conf.get("temporal-pos","").split(",");
        String[] spatialPosArray = ( conf.get("spatial-pos", "").equals("") ) ? new String[0] : conf.get("spatial-pos", "").split(",");
        String[] xPositionsArray = ( conf.get("xPositions", "").equals("") ) ? new String[0] : conf.get("xPositions", "").split(",");
        String[] yPositionsArray = ( conf.get("yPositions", "").equals("") ) ? new String[0] : conf.get("yPositions", "").split(",");
        
        temporalPos = FrameworkUtils.getIntArray(temporalArray);
        spatialPos = FrameworkUtils.getIntArray(spatialPosArray);
        xPositions = FrameworkUtils.getIntArray(xPositionsArray);
        yPositions = FrameworkUtils.getIntArray(yPositionsArray);
        
        // reading header
        Path header = new Path(conf.get("header", ""));
        
        if (s3)
        	fs = FileSystem.get(header.toUri(), conf);
        br = new BufferedReader(new InputStreamReader(fs.open(header)));
        parameterNames = br.readLine().split(",", -1);
        br.close();
        if (s3)
        	fs.close();
        
        /**
         * Spatial Resolution
         */
        
        switch (currentSpatialResolution) {
        
        case FrameworkUtils.POINTS:
            spatialTranslation = SpatialResolutionUtils.pointsResolution(spatialResolution,
                    gridResolution, xPositions, yPositions, conf);
            break;
        case FrameworkUtils.NBHD:
            spatialTranslation = SpatialResolutionUtils.nbhdResolution(spatialResolution,
                    spatialPos);
            break;
        case FrameworkUtils.ZIP:
            spatialTranslation = SpatialResolutionUtils.zipResolution(spatialResolution,
                    spatialPos, true, conf);
            break;
        case FrameworkUtils.GRID:
            spatialTranslation = SpatialResolutionUtils.gridResolution(spatialResolution,
                    spatialPos);
            break;
        case FrameworkUtils.CITY:
            spatialTranslation = new ToCity(spatialPos);
            break;
        default:
            System.out.println("Something is wrong...");
            System.exit(-1);
            break;
        }
        
    }
  
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	
        String[] input;
        
        try {
            input = FrameworkUtils.splitStr(value.toString(),
                    parameterNames.length);
        } catch (IOException e) {
            System.out.println("Error while parsing line: " + e.getLocalizedMessage());
            return;
        }
        
        /**
         * Spatial Resolution
         */
        
        ArrayList<Integer> spatial = spatialTranslation.translate(input);
        
        /**
         * Temporal Resolution
         */
        
        ArrayList<Integer> temporal = new ArrayList<Integer>();
        
        //Integer time = null;
        
        for (int tempPos: temporalPos) {
            int temp = FrameworkUtils.getTime(temporalResolution, input, tempPos);
            if (temp != -1) {
                //if (time == null) {
                //    time = FrameworkUtils.getTime(input, tempPos);
                //}
                temporal.add(temp);
            }
        }
        
        if ((spatial.size() <= 0) || (temporal.size() <= 0)) {
        	System.out.println("Spatial size: " + spatial.size() + " | Temporal size: " + temporal.size());
            return;
        }
        
        if ((spatial.size() != sizeSpatioTemp) || (temporal.size() != sizeSpatioTemp)) {
        	System.out.println("Spatial size: " + spatial.size() + " | Temporal size: " + temporal.size());
            return;
        }
        
        records++;
            
        // identifying all the aggregates for each parameter
        // done only once
        if (!aggregatesInit) {
            identifyAggregates(input);
            aggregatesInit = true;
        }
        
        /*
         *  getting the parameters
         *  null and default values are ignored
         */
        
        ArrayList<Aggregation> output = new ArrayList<Aggregation>();
        Iterator<Integer> it = aggregatesIndex.iterator();
        Float defaultVal;
        while (it.hasNext()) {
            int uniqueIndex = it.next();
            int index = attributeIndex.get(uniqueIndex);
            Float floatVal = 0f;
            
            // count
            if (index == -1) {
                Count agg = new Count();
                agg.addValue(floatVal, 0);
                output.add(agg);
                continue;
            }
            
            // count gradient
            if (index == -2) {
                CountGradient agg = new CountGradient();
                agg.addValue(floatVal, temporal.get(0));
                output.add(agg);
                continue;
            }
            
            // others
            try {
                floatVal = Float.parseFloat(input[index]);
                if (floatVal == null)
                    floatVal = Float.NaN;
            } catch (NumberFormatException e) {
                floatVal = Float.NaN;
            }
            if (!paramDefaults[index].equals("NONE")) {
                try {
                    defaultVal = Float.parseFloat(paramDefaults[index]);
                    if (floatVal.equals(defaultVal))
                        floatVal = Float.NaN;
                } catch (NumberFormatException e) {}
            }
            
            Aggregation agg = FrameworkUtils.getAggregation(aggregateFunctions.get(uniqueIndex));
            // TODO: only gets the first temporal attribute
            //agg.addValue(floatVal, time);
            agg.addValue(floatVal, temporal.get(0));
            output.add(agg);
        }
        
        keyWritable = new MultipleSpatioTemporalWritable(spatial, temporal);
        valueWritable = new AggregationArrayWritable(output);
        context.write(keyWritable, valueWritable);
    }
    
    @Override
    public void cleanup(Context context) throws IOException {
        
    	if (records > 0) {
    	    conf = context.getConfiguration();
    	    Path headerFile = null;
    	    FileSystem fs = null;
    	    
    	    if (s3) {
    	        headerFile = new Path(conf.get("aggregates", ""));
    	        fs = FileSystem.get(headerFile.toUri(), conf);
    	    } else {
    	        fs = FileSystem.get(new Configuration());
    	        headerFile = new Path(fs.getHomeDirectory() + "/"
    	                + context.getConfiguration().get("aggregates", ""));
    	    }
	        
	        // we cannot have multiple mappers writing the same file
	        if (fs.exists(headerFile)) {
	        	if (s3)
	        		fs.close();
	            return;
	        }
	        FSDataOutputStream fsDataOutputStream = fs.create(headerFile);
	        
	        String output = "";
	                
	        for (int i = 0; i < keyNames.length; i++) {
	            if (keyNames[i] != null)
	                output += keyNames[i] + ",";
	        }
	        
	        output = output.substring(0, output.length()-1) + "\t";
	        
	        Iterator<Integer> it = aggregatesIndex.iterator();
	        while (it.hasNext()) {
	            Integer index = it.next();
	            output += aggregates.get(index) + ",";
	        }
	        
	        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream));
	        bw.write(output.substring(0, output.length()-1) + "\n");
	        bw.write(String.valueOf(nbParameters));
	        bw.close();
	        if (s3)
	        	fs.close();
    	}
    }
}
