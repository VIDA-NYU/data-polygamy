/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.feature_identification;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import edu.nyu.vida.data_polygamy.ctdata.SpatioTemporalVal;
import edu.nyu.vida.data_polygamy.ctdata.TopologicalIndex;
import edu.nyu.vida.data_polygamy.ctdata.TopologicalIndex.Attribute;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AttributeResolutionWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Function;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalFloatWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TopologyTimeSeriesWritable;
import edu.nyu.vida.data_polygamy.utils.Utilities;

public class IndexCreationReducer extends Reducer<AttributeResolutionWritable, SpatioTemporalFloatWritable, AttributeResolutionWritable, TopologyTimeSeriesWritable> {
//public class IndexCreationReducer extends Reducer<AttributeResolutionWritable, SpatioTemporalFloatWritable, Text, Text> {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    boolean s3 = true;
    
    HashMap<Integer, HashSet<Integer>> functions = new HashMap<Integer, HashSet<Integer>>(); 
    HashMap<Integer,String> idToDataset = new HashMap<Integer,String>();
    HashMap<Integer,HashMap<Integer,String>> idToRegThreshold = new HashMap<Integer,HashMap<Integer,String>>();
    HashMap<Integer,HashMap<Integer,String>> idToRareThreshold = new HashMap<Integer,HashMap<Integer,String>>();
    HashSet<String> useMergeTree = new HashSet<String>(); 

    // threshold for outliers
    float th = 0.9f;
    
    // CITY, NBHD, ZIP
    int[][] nbhdEdges = new int[0][0];
    int[][] zipEdges = new int[0][0];
    int[][] blockEdges = new int[0][0];
    int nvCity = 1;
    int nvNbhd = 1;
    int nvZip = 1;
    int nvBlock = 1;
    
    TopologyTimeSeriesWritable valueWritable = new TopologyTimeSeriesWritable();
    
    // input stream
    ObjectInputStream inputStream;
    
    // output stream
    ObjectOutputStream outputStream;
    
    private MultipleOutputs<AttributeResolutionWritable,TopologyTimeSeriesWritable> out;
    //private MultipleOutputs<Text,Text> out;
    
    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        
        String[] datasetNames = conf.get("dataset-name","").split(",");
        String[] datasetIds = conf.get("dataset-id","").split(",");
        for (int i = 0; i < datasetNames.length; i++) {
            int dt = Integer.parseInt(datasetIds[i]);
            idToDataset.put(dt, datasetNames[i]);
            
            String regThresholds = conf.get("regular-" + datasetIds[i], "");
            if (!regThresholds.equals("")) {
                HashMap<Integer,String> attRegThresholds = new HashMap<Integer,String>();
                for (String keyVals : regThresholds.split(",")) {
                    String[] keyVal = keyVals.split("-");
                    attRegThresholds.put(Integer.parseInt(keyVal[0]), keyVal[1]);
                }
                idToRegThreshold.put(dt, attRegThresholds);
            }
            
            String rareThresholds = conf.get("rare-" + datasetIds[i], "");
            if (!rareThresholds.equals("")) {
                HashMap<Integer,String> attRareThresholds = new HashMap<Integer,String>();
                for (String keyVals : rareThresholds.split(",")) {
                    String[] keyVal = keyVals.split("-");
                    attRareThresholds.put(Integer.parseInt(keyVal[0]), keyVal[1]);
                }
                idToRareThreshold.put(dt, attRareThresholds);
            }
            
            // header
            
            String[] aggregates = context.getConfiguration().get("dataset-" + dt +
                    "-aggregates", "").split(",");
            
            HashSet<Integer> gradientFunctions = new HashSet<Integer>();
            
            for (int j = 0; j < aggregates.length; j++) {
                String[] vals = aggregates[j].split("-");
                int id = Integer.parseInt(vals[0]);
                String function = vals[1];
                if (function.contains(FrameworkUtils.functionToString(Function.GRADIENT))) {
                    gradientFunctions.add(id);
                } else if (function.contains(FrameworkUtils.functionToString(Function.COUNT_GRADIENT))) {
                    gradientFunctions.add(id);
                }
            }
            
            functions.put(dt, gradientFunctions);
            
        }
        
        String[] useMergeTreeStr = conf.get("use-merge-tree","").split(",");
        for (String dt : useMergeTreeStr) {
            useMergeTree.add(dt);
        }
    	
    	out = new MultipleOutputs<AttributeResolutionWritable,TopologyTimeSeriesWritable>(context);
    	//out = new MultipleOutputs<Text,Text>(context);

        String bucket = conf.get("bucket", "");
        
        String[] spatialResolutionArray = utils.getSpatialResolutions();
        for (int j = 0; j < spatialResolutionArray.length; j++) {
        	int spatialRes = utils.spatialResolution(spatialResolutionArray[j]);
        	
        	if ((spatialRes == FrameworkUtils.NBHD) || (spatialRes == FrameworkUtils.ZIP)
        	        || (spatialRes == FrameworkUtils.BLOCK)) {
        
	            if (bucket.equals(""))
	                s3 = false;
	            Path edgesPath = null;
	            
	            // reading nodes
	            if (spatialRes == FrameworkUtils.NBHD)
	                edgesPath = new Path(bucket + "neighborhood-graph");
	            else if (spatialRes == FrameworkUtils.ZIP)
	                edgesPath = new Path(bucket + "zipcode-graph");
	            else
	                edgesPath = new Path(bucket + "block-graph");
	            
	            FileSystem fs = null;
	            
	            if (s3)
	                fs = FileSystem.get(edgesPath.toUri(), conf);
	            else
	                fs = FileSystem.get(new Configuration());
	            
	            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(edgesPath)));
	            String [] s = Utilities.splitString(reader.readLine().trim());
	            if (spatialRes == FrameworkUtils.NBHD)
	                nvNbhd = Integer.parseInt(s[0].trim());
	            else if (spatialRes == FrameworkUtils.ZIP)
	                nvZip = Integer.parseInt(s[0].trim());
	            else
	                nvBlock = Integer.parseInt(s[0].trim());
	            
	            int ne = Integer.parseInt(s[1].trim());
    	        if (spatialRes == FrameworkUtils.NBHD)
                    nbhdEdges = new int[ne][2];
                else if (spatialRes == FrameworkUtils.ZIP)
                    zipEdges = new int[ne][2];
                else
                    blockEdges = new int[ne][2];
	            for(int i = 0;i < ne;i ++) {
	                s = Utilities.splitString(reader.readLine().trim());
	                int v1 = Integer.parseInt(s[0].trim());
	                int v2 = Integer.parseInt(s[1].trim());
	                if(v1 == v2) {
	                    continue;
	                }
	                if (spatialRes == FrameworkUtils.NBHD) {
	                	nbhdEdges[i][0] = v1;
	                	nbhdEdges[i][1] = v2;
	                } else if (spatialRes == FrameworkUtils.ZIP) {
	                    zipEdges[i][0] = v1;
	                    zipEdges[i][1] = v2;
	                } else {
	                    blockEdges[i][0] = v1;
	                    blockEdges[i][1] = v2;
	                }
	            }
	            reader.close();
	        }
        }
    }
    
    @Override
    public void reduce(AttributeResolutionWritable key, Iterable<SpatioTemporalFloatWritable> values, Context context)
            throws IOException, InterruptedException {
        
        Attribute att = new Attribute();
        int spatialRes = key.getSpatialResolution();
        int tempRes = key.getTemporalResolution();
        
        TopologicalIndex index = new TopologicalIndex();
        if (useMergeTree.contains(idToDataset.get(key.getDataset()))) {
            index = reduceMergeTreeUsage(key, context, att, tempRes, spatialRes);
        } else {
            att.id = key.getAttribute();
            index = reduceMergeTreeCreation(key, values, context, att, tempRes, spatialRes);
        }
        if (index.empty) return;
        
        // non-outliers
        String regThreshold  = "";
        //float regThresholdFloat = 0;
        if (idToRegThreshold.containsKey(key.getDataset())) {
            if (idToRegThreshold.get(key.getDataset()).containsKey(key.getAttribute())) {
                regThreshold = idToRegThreshold.get(key.getDataset()).get(key.getAttribute());
            }
        }
        ArrayList<byte[]> events = index.queryEvents(this.th, false, att, regThreshold);
        
        ArrayList<float[]> minTh = new ArrayList<float[]>();
        ArrayList<float[]> maxTh = new ArrayList<float[]>();
        ArrayList<float[]> points = new ArrayList<float[]>();
        getMinMaxPoints(att, values, tempRes, index.stTime, index.enTime, index.nv,
                minTh, maxTh, points);
        
        for (int spatial = 0; spatial < events.size(); spatial++) {
            //if (!att.nodeSet.contains(spatial))
            //    continue;
            
            valueWritable = new TopologyTimeSeriesWritable(
                    spatial,
                    key.getDataset(),
                    events.get(spatial),
                    minTh.get(spatial),
                    maxTh.get(spatial),
                    points.get(spatial),
                    index.stTime,
                    index.enTime,
                    false);
            out.write(key, valueWritable,
                    generateFileName(idToDataset.get(key.getDataset())));
            //out.write(new Text(key.toString()), new Text(valueWritable.toString()),
            //        generateFileName(idToDataset.get(key.getDataset())));
        }
        
        // outliers
        String rareThreshold  = "";
        //float rareThresholdFloat = 0;
        if (idToRareThreshold.containsKey(key.getDataset())) {
            if (idToRareThreshold.get(key.getDataset()).containsKey(key.getAttribute())) {
                rareThreshold = idToRareThreshold.get(key.getDataset()).get(key.getAttribute());
            }
        }
        events = index.queryEvents(this.th, true, att, rareThreshold);
        
        minTh = new ArrayList<float[]>();
        maxTh = new ArrayList<float[]>();
        points = new ArrayList<float[]>();
        getMinMaxPoints(att, values, tempRes, index.stTime, index.enTime, index.nv,
                minTh, maxTh, points);
        
        for (int spatial = 0; spatial < events.size(); spatial++) {
            //if (!att.nodeSet.contains(spatial))
            //    continue;
            
            valueWritable = new TopologyTimeSeriesWritable(
                    spatial,
                    key.getDataset(),
                    events.get(spatial),
                    minTh.get(spatial),
                    maxTh.get(spatial),
                    points.get(spatial),
                    index.stTime,
                    index.enTime,
                    true);
            out.write(key, valueWritable,
                    generateFileName(idToDataset.get(key.getDataset())));
            //out.write(new Text(key.toString()), new Text(valueWritable.toString()),
            //        generateFileName(idToDataset.get(key.getDataset())));
        }
    }

    private void getMinMaxPoints(Attribute att, Iterable<SpatioTemporalFloatWritable> values,
            int tempRes, int stTime, int enTime, int nv,
            ArrayList<float[]> minTh, ArrayList<float[]> maxTh, ArrayList<float[]> points) {
        
        int timeSteps = FrameworkUtils.getTimeSteps(tempRes, stTime, enTime);
        
        for (int i = 0; i < nv; i++) {
            float[] minThData = new float[timeSteps];
            Arrays.fill(minThData, 0);
            minTh.add(minThData);
            
            float[] maxThData = new float[timeSteps];
            Arrays.fill(maxThData, 0);
            maxTh.add(maxThData);
            
            float[] pointsData = new float[timeSteps];
            Arrays.fill(pointsData, 0);
            points.add(pointsData);
        }
        
        Iterator<SpatioTemporalFloatWritable> it = values.iterator();
        SpatioTemporalFloatWritable st;
        DateTime date;
        while (it.hasNext()) {
            st = it.next();
            
            int spatial = st.getSpatial();
            int temporal = st.getTemporal();
            float val = st.getValue();
            
            // for each temporal bin
            date = new DateTime(((long)temporal)*1000, DateTimeZone.UTC);
            int hash = (tempRes == FrameworkUtils.HOUR) ? date.getYear()*100 + date.getMonthOfYear() :
                ((tempRes == FrameworkUtils.DAY) ? date.getYear()*100 + (date.getMonthOfYear()/4) : 1);
           
            int index = FrameworkUtils.getTimeSteps(tempRes, stTime, temporal);
            
            // min th
            float[] minThData = minTh.get(spatial);
            minThData[index-1] = att.minThreshold.get(hash);
            minTh.set(spatial, minThData);
            
            // max th
            float[] maxThData = maxTh.get(spatial);
            maxThData[index-1] = att.maxThreshold.get(hash);
            maxTh.set(spatial, maxThData);
            
            // points
            float[] pointsData = points.get(spatial);
            pointsData[index-1] = val;
            points.set(spatial, pointsData);
        }
        
    }

    /**
     * Reduce function if merge tree for the dataset must be created and stored.
     */
    public TopologicalIndex reduceMergeTreeCreation(AttributeResolutionWritable key, Iterable<SpatioTemporalFloatWritable> values,
            Context context, Attribute att, int tempRes, int spatialRes) throws IOException {
        
        int datasetId = key.getDataset();
        int attributeId = key.getAttribute();
        boolean isGradient = functions.get(datasetId).contains(attributeId) ? true : false;
        
        Iterator<SpatioTemporalFloatWritable> it = values.iterator();
        SpatioTemporalFloatWritable st;
        DateTime date;
        while (it.hasNext()) {
            st = it.next();
            
            int spatial = st.getSpatial();
            int temporal = st.getTemporal();
            float val = st.getValue();
            
            att.nodeSet.add(spatial);
            
            // for each temporal bin
            date = new DateTime(((long)temporal)*1000, DateTimeZone.UTC);
            int hash = 1;
            if (!isGradient) {
                hash = (tempRes == FrameworkUtils.HOUR) ? date.getYear()*100 + date.getMonthOfYear() :
                    ((tempRes == FrameworkUtils.DAY) ? date.getYear()*100 + (date.getMonthOfYear()/4) : 1);
            }
            ArrayList<SpatioTemporalVal> temporalBinVals = att.data.get(hash);
            if (temporalBinVals == null)
                temporalBinVals = new ArrayList<SpatioTemporalVal>(); 
            
            // add val
            SpatioTemporalVal point = new SpatioTemporalVal(spatial, temporal, val);
            temporalBinVals.add(point);
            
            att.data.put(hash, temporalBinVals);
        }
        
        for (ArrayList<SpatioTemporalVal> stVal: att.data.values())
            Collections.sort(stVal);
        
        TopologicalIndex index = (spatialRes == FrameworkUtils.NBHD) ?
                new TopologicalIndex(spatialRes, tempRes, this.nvNbhd) :
                    ((spatialRes == FrameworkUtils.ZIP) ? new TopologicalIndex(spatialRes, tempRes, this.nvZip) :
                        ((spatialRes == FrameworkUtils.BLOCK) ? new TopologicalIndex(spatialRes, tempRes, this.nvBlock) :
                            new TopologicalIndex(spatialRes, tempRes, this.nvCity)));
        int ret = (spatialRes == FrameworkUtils.NBHD) ? index.createIndex(att, this.nbhdEdges) :
            ((spatialRes == FrameworkUtils.BLOCK) ? index.createIndex(att, this.blockEdges) :
                index.createIndex(att, this.zipEdges));
        
        if (ret == 1) {
            return new TopologicalIndex();
        }
        
        return index;
    }
    
    /**
     * Reduce function if merge tree for the dataset has already been created before.
     */
    public TopologicalIndex reduceMergeTreeUsage(AttributeResolutionWritable key, Context context, Attribute att,
            int tempRes, int spatialRes) throws IOException {
        
        // loading topological index
        TopologicalIndex index = new TopologicalIndex();
        String fileName = generateIndexFileName(idToDataset.get(key.getDataset()), key.getAttribute(), tempRes, spatialRes);
        try {
            inputStream = new ObjectInputStream(FrameworkUtils.openFile(fileName, context.getConfiguration(), s3));
            index = (TopologicalIndex) inputStream.readObject();
            Attribute savedAtt = (Attribute) inputStream.readObject();
            att.copy(savedAtt);
            inputStream.close();
        } catch (ClassNotFoundException e) {
            System.out.println("Something went wrong... Cannot read merge tree: " + fileName);
            System.exit(1);
        }
        
        return index;
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    	out.close();
    }
    
    private String generateFileName(String dataset) {
    	return (dataset + "/data");
    }
    
    private String generateIndexFileName(String dataset, int att, int tempRes, int spatialRes) {
        return (FrameworkUtils.mergeTreeDir + "/" + dataset + "/" + att + "-" + tempRes + "-" + spatialRes);
    }
}
