/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.relationship_computation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Months;
import org.joda.time.Weeks;
import org.joda.time.Years;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.PairAttributeWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TimeSeriesStats;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TopologyTimeSeriesWritable;
import edu.nyu.vida.data_polygamy.utils.SpatialGraph;

public class CorrelationReducer extends Reducer<PairAttributeWritable, TopologyTimeSeriesWritable, Text, Text> {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    
    Configuration conf;
    int dataset1, dataset2, spatial, temporal;
    int dataset1Key = 0;
    int dataset2Key = 1;
    String fileName;
    HashMap<Integer,String> datasets = new HashMap<Integer,String>();
    
    float scoreThreshold;
    float strengthThreshold;
    boolean hasScoreThreshold = false;
    boolean hasStrengthThreshold = false;
    boolean removeNotSignificant = false;
    boolean outputIds = false;
    
    // spatial information
    SpatialGraph spatialGraph = new SpatialGraph();
    SpatialGraph nbhdGraph = new SpatialGraph();
    SpatialGraph zipGraph = new SpatialGraph();
    SpatialGraph blockGraph = new SpatialGraph();
    int gridSize = 0;
    int[][] originalGrid;
    boolean isBlock = false;
    boolean isNbhd = false;
    boolean isGrid = false;
    boolean isZip = false;
    boolean isCity = false;
    
    // temporal information
    boolean isHour = false;
    boolean isDay = false;
    boolean isWeek = false;
    boolean isMonth = false;
    boolean isYear = false;
    
    // randomization
    boolean completeRandomization = false;
    String randomizationStr;
    int size = 0;
    
    // header
    HashMap<Integer,HashMap<Integer, String>> header = new HashMap<Integer,HashMap<Integer, String>>();
    
    // Monte Carlo permutation test
    //  null hypothesis (H0): random predictability
    //  we want to reject H0
    //  p-value is the rate of repetitions in the
    //    permutation test that have high score
    //    (i.e., if p-value is high, it means that
    //    a high score can be computed by random;
    //    in other words, a high p-value confirms H0)
    //  we reject H0 if p-value <= alpha
    float alpha = 0.05f;
    int repetitions = 1000;
    
    ArrayList<TopologyTimeSeriesWritable[]> timeSeriesPerSpatial = new ArrayList<TopologyTimeSeriesWritable[]>();
    
    Text keyWritable = new Text();
    Text valueWritable = new Text();
    
    boolean s3 = true;
    private MultipleOutputs<Text,Text> out;
    
    private void resolutionHandler(int spatialResolution, int temporalResolution) {
    	spatial = spatialResolution;
        temporal = temporalResolution;
        
        // resetting
        isBlock = false;
        isNbhd = false;
        isZip = false;
        isGrid = false;
        isCity = false;
        
        isHour = false;
        isDay = false;
        isWeek = false;
        isMonth = false;
        isYear = false;
        
        switch(spatial) {
        case FrameworkUtils.NBHD:
            spatialGraph.init(nbhdGraph);
            isNbhd = true;
            break;
        case FrameworkUtils.ZIP:
        	spatialGraph.init(zipGraph);
        	isZip = true;
        	break;
        case FrameworkUtils.BLOCK:
            spatialGraph.init(blockGraph);
            isBlock = true;
            break;
        case FrameworkUtils.GRID:
            isGrid = true;
            break;
        case FrameworkUtils.CITY:
            isCity = true;
            break;
        default:
            isCity = true;
            break;
        }
        
        switch(temporal) {
        case FrameworkUtils.HOUR:
            isHour = true;
            break;
        case FrameworkUtils.DAY:
            isDay = true;
            break;
        case FrameworkUtils.WEEK:
            isWeek = true;
            break;
        case FrameworkUtils.MONTH:
            isMonth = true;
            break;
        case FrameworkUtils.YEAR:
            isYear = true;
            break;
        default:
            isYear = true;
            break;
        }
    }
    
    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        
    	out = new MultipleOutputs<Text,Text>(context);
        conf = context.getConfiguration();
        
        String[] datasetIdsStr = conf.get("dataset-keys","").split(",");
        String[] datasetNames = conf.get("dataset-names","").split(",");
        
        if (datasetIdsStr.length != datasetNames.length) {
            System.out.println("Something went wrong... Number of ids should match number of datasets");
            System.exit(-1);
        }

        for (int i = 0; i < datasetIdsStr.length; i++) {
            int datasetId = Integer.parseInt(datasetIdsStr[i]);
            String[] datasetAggHeader = conf.get("dataset-" + datasetIdsStr[i] + "-agg", "").split(",");
            
            HashMap<Integer, String> headerTemp = new HashMap<Integer, String>(); 
            for (int j = 0; j < datasetAggHeader.length; j++) {
                int attribute = Integer.parseInt(datasetAggHeader[j].substring(0, datasetAggHeader[j].indexOf("-")));
                String name = datasetAggHeader[j].substring(datasetAggHeader[j].indexOf("-")+1, datasetAggHeader[j].length());
                headerTemp.put(attribute, name);
            }
            
            header.put(datasetId, headerTemp);
            datasets.put(datasetId, datasetNames[i]);
        }
        
        String scoreThresholdStr = conf.get("score-threshold", "");
        if (!scoreThresholdStr.isEmpty()) {
            hasScoreThreshold = true;
            scoreThreshold = Math.abs(Float.parseFloat(scoreThresholdStr));
        }
        
        String strengthThresholdStr = conf.get("strength-threshold", "");
        if (!strengthThresholdStr.isEmpty()) {
            hasStrengthThreshold = true;
            strengthThreshold = Math.abs(Float.parseFloat(strengthThresholdStr));
        }
        
        removeNotSignificant = Boolean.parseBoolean(conf.get("remove-not-significant"));
        completeRandomization = Boolean.parseBoolean(conf.get("complete-random"));
        randomizationStr = conf.get("complete-random-str","");
        outputIds = conf.getBoolean("output-ids", false);
        
        // nbhd grapgh
        nbhdGraph.init(FrameworkUtils.NBHD, conf);
        
        // zipcode graph
        zipGraph.init(FrameworkUtils.ZIP, conf);
        
        // block graph
        blockGraph.init(FrameworkUtils.BLOCK, conf);
        
        // grid
        gridSize = 2048;
        //gridSize = Integer.parseInt(conf.get("spatial-resolution").replace("grid", ""));
        originalGrid = new int[gridSize][gridSize];
        for (int j = 0; j < gridSize; j++) {
            for (int i = 0; i < gridSize; i++)
                originalGrid[i][j] = j * gridSize + i;
        }
    }
    
    @Override
    public void reduce(PairAttributeWritable key, Iterable<TopologyTimeSeriesWritable> values, Context context)
            throws IOException, InterruptedException {
    	
    	//long start = System.currentTimeMillis();
        
        timeSeriesPerSpatial.clear();
        resolutionHandler(key.getSpatialResolution(), key.getTemporalResolution());
        
        TopologyTimeSeriesWritable[] elem;
        switch(spatial) {
        case FrameworkUtils.GRID:
            for (int i = 0; i < gridSize; i++) {
                elem = new TopologyTimeSeriesWritable[2];
                elem[0] = null;
                elem[1] = null;
                timeSeriesPerSpatial.add(elem);
            }
            break;
        case FrameworkUtils.CITY:
            elem = new TopologyTimeSeriesWritable[2];
            elem[0] = null;
            elem[1] = null;
            timeSeriesPerSpatial.add(elem);
            break;
        default:
            for (int i = 0; i < spatialGraph.nbNodes(); i++) {
                elem = new TopologyTimeSeriesWritable[2];
                elem[0] = null;
                elem[1] = null;
                timeSeriesPerSpatial.add(elem);
            }
            break;
        }
        
        size = timeSeriesPerSpatial.size();
        
        // initializing some variables
        dataset1 = key.getFirstDataset();
        dataset2 = key.getSecondDataset();
        fileName = datasets.get(dataset1) + "-" + datasets.get(dataset2) + "/"
                + utils.temporalResolutionStr(key.getTemporalResolution()) + "-"
                + utils.spatialResolutionStr(key.getSpatialResolution()) + "-"
                + ((key.getIsOutlier()) ? "outliers" : "events") + "-"
                + randomizationStr + "/data";
        
        Iterator<TopologyTimeSeriesWritable> it = values.iterator();
        TopologyTimeSeriesWritable timeSeries;
        while (it.hasNext()) {
            timeSeries = it.next();
            int indexSpatial = timeSeries.getSpatial();
            int dataset = timeSeries.getDataset();
            
            int datasetKey = 0;
            if (dataset == dataset1)
                datasetKey = dataset1Key;
            else
                datasetKey = dataset2Key;
            
            elem = timeSeriesPerSpatial.get(indexSpatial);
            if (elem[datasetKey] != null) {
                System.out.println("Something went wrong... Data already filled");
                System.exit(-1);
            }
            elem[datasetKey] = new TopologyTimeSeriesWritable(timeSeries);
            timeSeriesPerSpatial.set(indexSpatial, elem);
        }
        
        /*
         * Aligned Score and Strength
         */
        
        TimeSeriesStats stats = new TimeSeriesStats();
        for (int i = 0; i < size; i++) {
            elem = timeSeriesPerSpatial.get(i);
            TimeSeriesStats tempStats = getStats(temporal, elem[dataset1Key], elem[dataset2Key], false); 
            stats.add(tempStats);
        }
        stats.computeScores();
        
        if (!stats.isIntersect())
            return;
        
        float alignedScore = stats.getRelationshipScore();
        float alignedStrength = stats.getRelationshipStrength();
        
        if ((hasScoreThreshold) && (Math.abs(alignedScore) < scoreThreshold)) return;
        if ((hasStrengthThreshold) && (Math.abs(alignedStrength) < strengthThreshold)) return;
        
        float nMatchEvents = stats.getMatchEvents();
        float nMatchPosEvents = stats.getMatchPosEvents();
        float nMatchNegEvents = stats.getMatchNegEvents();
        float nPosFirstNonSecond = stats.getPosFirstNonSecond();
        float nNegFirstNonSecond = stats.getNegFirstNonSecond();
        float nNonFirstPosSecond = stats.getNonFirstPosSecond();
        float nNonFirstNegSecond = stats.getNonFirstNegSecond();
        
        //long end = System.currentTimeMillis();
        
        /*
         * Monte Carlo Permutation Test
         */
        
        float pValue = 0;
        ArrayList<Integer[]> pairs = new ArrayList<Integer[]>();
        
        //long start2 = System.currentTimeMillis();
        
        switch(spatial) {
        
        case FrameworkUtils.GRID:
            
            for (int j = 0; j < repetitions; j++) {
                pairs.clear();
                pairs = (completeRandomization) ? spatialCompleteRandom() : toroidalShift();
                
                stats = new TimeSeriesStats();
                for (int i = 0; i < pairs.size(); i++) {
                    Integer[] pair = pairs.get(i);
                    stats.add(getStats(temporal, timeSeriesPerSpatial.get(pair[0])[dataset1Key],
                            timeSeriesPerSpatial.get(pair[1])[dataset2Key], false));
                }
                stats.computeScores();
                
                float mcScore = stats.getRelationshipScore();
                //float mcStrength = stats.getRelationshipStrength();
                
                if (alignedScore > 0) {
                    if (mcScore >= alignedScore)
                        pValue += 1;
                }
                else {
                    if (mcScore <= alignedScore)
                        pValue += 1;
                }
                if (pValue > (alpha*repetitions)) break; // pruning
            }
            
            pValue = pValue/((float)(repetitions));
            if ((!removeNotSignificant) || ((pValue <= alpha) && (removeNotSignificant))) {
                emitKeyValue(outputIds, key, alignedScore, alignedStrength, pValue, nMatchEvents,
                        nMatchPosEvents, nMatchNegEvents, nPosFirstNonSecond,
                        nNegFirstNonSecond, nNonFirstPosSecond, nNonFirstNegSecond);
            }
            
            break;
            
        case FrameworkUtils.CITY:
            
            if (timeSeriesPerSpatial.size() > 1) {
                System.out.println("Something went wrong... There should be only one spatial element.");
                System.exit(-1);
            }
            
            elem = timeSeriesPerSpatial.get(0);
            
            for (int j = 0; j < repetitions; j++) {
            	//long startCity = System.currentTimeMillis();
                stats = new TimeSeriesStats();
                stats.add(getStats(temporal, elem[dataset1Key], elem[dataset2Key], true));
                
                stats.computeScores();
                
                float mcScore = stats.getRelationshipScore();
                //float mcStrength = stats.getRelationshipStrength();
                
                if (alignedScore > 0) {
                    if (mcScore >= alignedScore)
                        pValue += 1;
                }
                else {
                    if (mcScore <= alignedScore)
                        pValue += 1;
                }
                if (pValue > (alpha*repetitions)) break; // pruning
                //long endCity = System.currentTimeMillis();
                //if (j == 0) System.out.println("One repetition: " + (endCity-startCity) + " ms");
            }
            
            pValue = pValue/((float)(repetitions));
            if ((!removeNotSignificant) || ((pValue <= alpha) && (removeNotSignificant))) {
                emitKeyValue(outputIds, key, alignedScore, alignedStrength, pValue, nMatchEvents,
                        nMatchPosEvents, nMatchNegEvents, nPosFirstNonSecond,
                        nNegFirstNonSecond, nNonFirstPosSecond, nNonFirstNegSecond);
            }
        
            break;
            
        default:
            
            for (int j = 0; j < repetitions; j++) {
                //long startNbhd = System.currentTimeMillis();
                pairs.clear();
                pairs = (completeRandomization) ? spatialCompleteRandom() : bfsShift();
                
                stats = new TimeSeriesStats();
                for (int i = 0; i < pairs.size(); i++) {
                    Integer[] pair = pairs.get(i);
                    stats.add(getStats(temporal, timeSeriesPerSpatial.get(pair[0])[dataset1Key],
                            timeSeriesPerSpatial.get(pair[1])[dataset2Key], false));
                }
                stats.computeScores();
                
                float mcScore = stats.getRelationshipScore();
                //float mcStrength = stats.getRelationshipStrength();
                
                if (alignedScore > 0) {
                    if (mcScore >= alignedScore)
                        pValue += 1;
                }
                else {
                    if (mcScore <= alignedScore)
                        pValue += 1;
                }
                if (pValue > (alpha*repetitions)) break; // pruning
                //long endNbhd = System.currentTimeMillis();
                //if (j == 0) System.out.println("One repetition: " + (endNbhd-startNbhd) + " ms");
            }
            
            pValue = pValue/((float)(repetitions));
            if ((!removeNotSignificant) || ((pValue <= alpha) && (removeNotSignificant))) {
                emitKeyValue(outputIds, key, alignedScore, alignedStrength, pValue, nMatchEvents,
                        nMatchPosEvents, nMatchNegEvents, nPosFirstNonSecond,
                        nNegFirstNonSecond, nNonFirstPosSecond, nNonFirstNegSecond);
            }
            
            break;
        }
        
    }
    
    private void emitKeyValue(boolean outputIds, PairAttributeWritable key, float score, float strength, 
            float pValue, float nMatchEvents, float nMatchPosEvents,
            float nMatchNegEvents, float nPosFirstNonSecond,
            float nNegFirstNonSecond, float nNonFirstPosSecond,
            float nNonFirstNegSecond)
                    throws IOException, InterruptedException {
        if (outputIds) {
            keyWritable = new Text(key.getFirstDataset() + "," + key.getFirstAttribute() +
                    "," + key.getSecondDataset() + "," + key.getSecondAttribute() +
                    "," + key.getSpatialResolution() + "," + key.getTemporalResolution());
        } else {
            keyWritable = new Text(header.get(dataset1).get(key.getFirstAttribute()) + "," +
                        header.get(dataset2).get(key.getSecondAttribute()));
        }
        valueWritable = new Text(score + "," + strength + "," + pValue + ","
                + nMatchEvents + "," + nMatchPosEvents + "," + nMatchNegEvents + "," + nPosFirstNonSecond
                + "," + nNegFirstNonSecond + "," + nNonFirstPosSecond + "," + nNonFirstNegSecond);
        out.write(keyWritable, valueWritable, fileName);
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    	out.close();
    }
    
    public static TimeSeriesStats getStats(int temporal, TopologyTimeSeriesWritable timeSeries1,
            TopologyTimeSeriesWritable timeSeries2, boolean temporalPermutationTest) {
        
        TimeSeriesStats output = new TimeSeriesStats();
        
        if ((timeSeries1 == null) || (timeSeries2 == null))
            return output;
        
        // detecting intersection
        
        long start1 = timeSeries1.getStart();
        long end1 = timeSeries1.getEnd();
        long start2 = timeSeries2.getStart();
        long end2 = timeSeries2.getEnd();
        
        if (((end1 < start2) && (start1 < start2)) || ((end1 > end2) && (start1 > end2)))
            return output;
        
        output.setIntersect(true);
        
        DateTime start1Obj = new DateTime(start1*1000, DateTimeZone.UTC);
        DateTime end1Obj = new DateTime(end1*1000, DateTimeZone.UTC);
        DateTime start2Obj = new DateTime(start2*1000, DateTimeZone.UTC);
        DateTime end2Obj = new DateTime(end2*1000, DateTimeZone.UTC);
        
        byte[] eventTimeSeries1 = timeSeries1.getTimeSeries();
        byte[] eventTimeSeries2 = timeSeries2.getTimeSeries();
        
        int startRange = 0;
        int endRange = 0;
        
        switch(temporal) {
        case FrameworkUtils.HOUR:
            startRange = (start1 > start2) ? Hours.hoursBetween(start2Obj, start1Obj).getHours() :
                Hours.hoursBetween(start1Obj, start2Obj).getHours();
            endRange = (end1 > end2) ? Hours.hoursBetween(end2Obj, end1Obj).getHours() :
                Hours.hoursBetween(end1Obj, end2Obj).getHours();
            break;
        case FrameworkUtils.DAY:
            startRange = (start1 > start2) ? Days.daysBetween(start2Obj, start1Obj).getDays() :
                Days.daysBetween(start1Obj, start2Obj).getDays();
            endRange = (end1 > end2) ? Days.daysBetween(end2Obj, end1Obj).getDays() :
                Days.daysBetween(end1Obj, end2Obj).getDays();
            break;
        case FrameworkUtils.WEEK:
            startRange = (start1 > start2) ? Weeks.weeksBetween(start2Obj, start1Obj).getWeeks() :
                Weeks.weeksBetween(start1Obj, start2Obj).getWeeks();
            endRange = (end1 > end2) ? Weeks.weeksBetween(end2Obj, end1Obj).getWeeks() :
                Weeks.weeksBetween(end1Obj, end2Obj).getWeeks();
            break;
        case FrameworkUtils.MONTH:
            startRange = (start1 > start2) ? Months.monthsBetween(start2Obj, start1Obj).getMonths() :
                Months.monthsBetween(start1Obj, start2Obj).getMonths();
            endRange = (end1 > end2) ? Months.monthsBetween(end2Obj, end1Obj).getMonths() :
                Months.monthsBetween(end1Obj, end2Obj).getMonths();
            break;
        case FrameworkUtils.YEAR:
            startRange = (start1 > start2) ? Years.yearsBetween(start2Obj, start1Obj).getYears() :
                Years.yearsBetween(start1Obj, start2Obj).getYears();
            endRange = (end1 > end2) ? Years.yearsBetween(end2Obj, end1Obj).getYears() :
                Years.yearsBetween(end1Obj, end2Obj).getYears();
            break;
        default:
            startRange = (start1 > start2) ? Hours.hoursBetween(start2Obj, start1Obj).getHours() :
                Hours.hoursBetween(start1Obj, start2Obj).getHours();
            endRange = (end1 > end2) ? Hours.hoursBetween(end2Obj, end1Obj).getHours() :
                Hours.hoursBetween(end1Obj, end2Obj).getHours();
            break;
        }
        
        int indexStart1 = (start2 > start1) ? startRange : 0;
        int indexStart2 = (start2 > start1) ? 0 : startRange;
        int indexEnd1 = (end2 > end1) ? eventTimeSeries1.length : eventTimeSeries1.length - endRange;
        int indexEnd2 = (end2 > end1) ? eventTimeSeries2.length - endRange : eventTimeSeries2.length;
        
        /*DateTime startIntersect = FrameworkUtils.addTime(temporal, indexStart1, start1Obj);
        if (!(startIntersect.isEqual(FrameworkUtils.addTime(temporal, indexStart2, start2Obj)))) {
            System.out.println("Something went wrong... Different starts");
            System.exit(-1);
        }*/
        
        byte[] timeSeries1Int = Arrays.copyOfRange(eventTimeSeries1, indexStart1, indexEnd1);
        byte[] timeSeries2Int = Arrays.copyOfRange(eventTimeSeries2, indexStart2, indexEnd2);
        
        if (timeSeries1Int.length != timeSeries2Int.length) {
            System.out.println("Something went wrong... Different sizes");
            System.exit(-1);
        }
        
        int nMatchEvents = 0;
        int nMatchPosEvents = 0;
        int nMatchNegEvents = 0;
        int nPosFirstNonSecond = 0;
        int nNegFirstNonSecond = 0;
        int nNonFirstPosSecond = 0;
        int nNonFirstNegSecond = 0;
        
        //String eventDateTime = null;
        int indexD1 = (temporalPermutationTest) ? new Random().nextInt(timeSeries1Int.length) : 0;
        int indexD2 = (temporalPermutationTest) ? new Random().nextInt(timeSeries2Int.length) : 0;
        for (int i = 0; i < timeSeries1Int.length; i++) {
            int j = (indexD1 + i) % timeSeries1Int.length;
            int k = (indexD2 + i) % timeSeries2Int.length;            
            byte result = (byte) (timeSeries1Int[j] | timeSeries2Int[k]);
            
            //eventDateTime = FrameworkUtils.getTemporalStr(
            //        temporal, FrameworkUtils.addTime(temporal, j, startIntersect));
            
            switch(result) {
            case FrameworkUtils.nonEventsMatch: // both non events
                // do nothing
                break;
            case FrameworkUtils.posEventsMatch: // both positive
                nMatchEvents++;
                nMatchPosEvents++;
                //output.addMatchPosEvents(eventDateTime);
                break;
            case FrameworkUtils.nonEventPosEventMatch: // one positive, one non-event
                if (timeSeries1Int[j] == FrameworkUtils.positiveEvent)
                    nPosFirstNonSecond++;
                else
                    nNonFirstPosSecond++;
                break;
            case FrameworkUtils.negEventsMatch: // both negative
                nMatchEvents++;
                nMatchPosEvents++;
                //output.addMatchPosEvents(eventDateTime);
                break;
            case FrameworkUtils.nonEventNegEventMatch: // one negative, one non-event
                if (timeSeries1Int[j] == FrameworkUtils.negativeEvent)
                    nNegFirstNonSecond++;
                else
                    nNonFirstNegSecond++;
                break;
            case FrameworkUtils.negEventPosEventMatch: // one negative, one positive
                nMatchEvents++;
                nMatchNegEvents++;
                //output.addMatchNegEvents(eventDateTime);
                break;
            default:
                System.out.println("Something went wrong... Wrong case");
                System.exit(-1);
            }
        }
        
        output.setParameters(
                nMatchEvents,
                nMatchPosEvents,
                nMatchNegEvents,
                nPosFirstNonSecond,
                nNegFirstNonSecond,
                nNonFirstPosSecond,
                nNonFirstNegSecond);
        
        return output;
    }
    
    private ArrayList<Integer[]> spatialCompleteRandom() {
        
        // each element inside the array list represents a pair
        ArrayList<Integer[]> result = new ArrayList<Integer[]>();
        
        ArrayList<Integer> vals = new ArrayList<Integer>();
        for (int k = 0; k < size; k++) {
            vals.add(k);
        }
        Collections.shuffle(vals);
        
        for (int k = 0; k < size; k++) {
            
            Integer[] pair = new Integer[2];
            pair[0] = vals.get(k);
            pair[1] = k;
            result.add(pair);
        }
        
        return result;
    }
    
    private ArrayList<Integer[]> toroidalShift() {
        
        // each element inside the array list represents a pair
        ArrayList<Integer[]> result = new ArrayList<Integer[]>();
        
        Random random = new Random();
        
        int x = random.nextInt(gridSize);
        int y = random.nextInt(gridSize);
        
        // shifted grid
        for (int j = 0; j < gridSize; j++) {
            for (int i = 0; i < gridSize; i++) {
                int newX = (x + i) % gridSize;
                int newY = (y + j) % gridSize;
                Integer[] pair = new Integer[2];
                pair[0] = originalGrid[i][j];
                pair[1] = originalGrid[newX][newY];
                result.add(pair);
            }
        }
        
        return result;
    }
    
    // TODO: we may not have all the neighborhoods for the data
    private ArrayList<Integer[]> bfsShift() {
        return spatialGraph.generateRandomShift();
    }
}
