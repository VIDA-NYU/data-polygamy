/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.standard_techniques;

import infodynamics.measures.continuous.kernel.EntropyCalculatorKernel;
import infodynamics.measures.continuous.kernel.MutualInfoCalculatorMultiVariateKernel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;

import net.sf.javaml.core.DenseInstance;
import net.sf.javaml.distance.fastdtw.dtw.DTW;
import net.sf.javaml.distance.fastdtw.timeseries.TimeSeries;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.PairAttributeWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalValueWritable;
import edu.nyu.vida.data_polygamy.utils.SpatialGraph;

public class CorrelationTechniquesReducer extends Reducer<PairAttributeWritable, SpatioTemporalValueWritable, Text, Text> {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    
    Configuration conf;
    int dataset1, dataset2, spatial, temporal;
    int dataset1Key = 0;
    int dataset2Key = 1;
    String fileName;
    HashMap<Integer,String> datasets = new HashMap<Integer,String>();
    
    // spatial information
    SpatialGraph spatialGraph = new SpatialGraph();
    SpatialGraph nbhdGraph = new SpatialGraph();
    SpatialGraph zipGraph = new SpatialGraph();
    int gridSize = 0;
    int[][] originalGrid;
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
    
    HashMap<Integer,String> idToDataset = new HashMap<Integer,String>();
    
    // header
    HashMap<Integer,HashMap<Integer, String>> header = new HashMap<Integer,HashMap<Integer, String>>();
    
    ArrayList<TreeMap<Integer, Float>>[] timeSeries;
    
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
    
    Text keyWritable = new Text();
    Text valueWritable = new Text();
    
    boolean s3 = true;
    private MultipleOutputs<Text,Text> out;
    
    private void resolutionHandler(int spatialResolution, int temporalResolution) {
    	spatial = spatialResolution;
        temporal = temporalResolution;
        
        // resetting
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
        
        // nbhd grapgh
        nbhdGraph.init(FrameworkUtils.NBHD, conf);
        
        // zipcode graph
        zipGraph.init(FrameworkUtils.ZIP, conf);
        
        // grid
        gridSize = 2048;
        //gridSize = Integer.parseInt(conf.get("spatial-resolution").replace("grid", ""));
        originalGrid = new int[gridSize][gridSize];
        for (int j = 0; j < gridSize; j++) {
            for (int i = 0; i < gridSize; i++)
                originalGrid[i][j] = j * gridSize + i;
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void reduce(PairAttributeWritable key, Iterable<SpatioTemporalValueWritable> values, Context context)
            throws IOException, InterruptedException {
        
        resolutionHandler(key.getSpatialResolution(), key.getTemporalResolution());
        
        int size = 0;
        switch(spatial) {
        case FrameworkUtils.NBHD:
            size = spatialGraph.nbNodes();
            break;
        case FrameworkUtils.ZIP:
            size = zipGraph.nbNodes();
            break;
        case FrameworkUtils.GRID:
            size = gridSize;
            break;
        case FrameworkUtils.CITY:
            size = 1;
            break;
        default:
            size = 1;
            break;
        }
        
        timeSeries = (ArrayList<TreeMap<Integer, Float>>[]) new ArrayList[size];
        for (int i = 0; i < size; i++) {
            ArrayList<TreeMap<Integer, Float>> spatialTimeSeries = new ArrayList<TreeMap<Integer, Float>>();
            spatialTimeSeries.add(new TreeMap<Integer, Float>()); // 0
            spatialTimeSeries.add(new TreeMap<Integer, Float>()); // 1
            timeSeries[i] = spatialTimeSeries;
        }
        
        // initializing some variables
        dataset1 = key.getFirstDataset();
        dataset2 = key.getSecondDataset();
        fileName = datasets.get(dataset1) + "-" + datasets.get(dataset2) + "/"
                + utils.temporalResolutionStr(key.getTemporalResolution()) + "-"
                + utils.spatialResolutionStr(key.getSpatialResolution()) + "/data";
        
        Iterator<SpatioTemporalValueWritable> it = values.iterator();
        SpatioTemporalValueWritable st;

        while (it.hasNext()) {
            st = it.next();
            
            int dataset = st.getDataset();
            int temporal = st.getTemporal();
            int spatial = st.getSpatial();
            float val = st.getValue();
            
            int datasetKey = 0;
            if (dataset == dataset1)
                datasetKey = dataset1Key;
            else
                datasetKey = dataset2Key;
            
            TreeMap<Integer, Float> map = timeSeries[spatial].get(datasetKey);
            map.put(temporal, val);
            timeSeries[spatial].set(datasetKey, map);
            
        }
        
        double corr = 0; // Pearsons Correlations
        double mi = 0; // Mutual Information
        double dtw = 0; // DTW
        
        int count = 0;
        for (int i = 0; i < size; i++) {
            double[] tempValues = computeCorrelationTechniques(timeSeries, i, i, false);
            if (tempValues == null)
                continue;
            corr += tempValues[0];
            mi += tempValues[1];
            dtw += tempValues[2];
            count++;
        }
        
        if (count == 0) return;
        
        corr = corr/count;
        mi = mi/count;
        dtw = dtw/count;
        
        /*
         * Monte Carlo Permutation Test
         */
        
        double pValueCorr = 0;
        double pValueMI = 0;
        double pValueDTW = 0;
        ArrayList<Integer[]> pairs = new ArrayList<Integer[]>();
        
        switch(spatial) {
        case FrameworkUtils.NBHD:
            for (int j = 0; j < repetitions; j++) {
                double mcCorr = 0;
                
                pairs.clear();
                pairs = bfsShift(true);
                
                count = 0;
                for (int i = 0; i < pairs.size(); i++) {
                    Integer[] pair = pairs.get(i);
                    double[] tempValues = computeCorrelationTechniques(timeSeries, pair[0], pair[1], false);
                    if (tempValues == null)
                        continue;
                    corr += tempValues[0];
                    //mi += tempValues[1];
                    //dtw += tempValues[2];
                    count++;
                }
                mcCorr = (count == 0) ? 0 : mcCorr/count;
                
                if (corr > 0) {
                    if (mcCorr >= corr)
                        pValueCorr += 1;
                }
                else {
                    if (mcCorr <= corr)
                        pValueCorr += 1;
                }
                if (pValueCorr > (alpha*repetitions)) break; // pruning
            }
            
            pValueCorr = pValueCorr/((double)(repetitions));
            emitKeyValue(key, corr, mi, dtw, pValueCorr, pValueMI, pValueDTW);
            break;
        case FrameworkUtils.ZIP:
            for (int j = 0; j < repetitions; j++) {
                double mcCorr = 0;
                
                pairs.clear();
                pairs = bfsShift(false);
                
                count = 0;
                for (int i = 0; i < pairs.size(); i++) {
                    Integer[] pair = pairs.get(i);
                    double[] tempValues = computeCorrelationTechniques(timeSeries, pair[0], pair[1], false);
                    if (tempValues == null)
                        continue;
                    corr += tempValues[0];
                    //mi += tempValues[1];
                    //dtw += tempValues[2];
                    count++;
                }
                mcCorr = (count == 0) ? 0 : mcCorr/count;
                
                if (corr > 0) {
                    if (mcCorr >= corr)
                        pValueCorr += 1;
                }
                else {
                    if (mcCorr <= corr)
                        pValueCorr += 1;
                }
                if (pValueCorr > (alpha*repetitions)) break; // pruning
            }
            
            pValueCorr = pValueCorr/((double)(repetitions));
            emitKeyValue(key, corr, mi, dtw, pValueCorr, pValueMI, pValueDTW);
            break;
        case FrameworkUtils.GRID:
            for (int j = 0; j < repetitions; j++) {
                double mcCorr = 0;
                
                pairs.clear();
                pairs = toroidalShift();
                
                count = 0;
                for (int i = 0; i < pairs.size(); i++) {
                    Integer[] pair = pairs.get(i);
                    double[] tempValues = computeCorrelationTechniques(timeSeries, pair[0], pair[1], false);
                    if (tempValues == null)
                        continue;
                    corr += tempValues[0];
                    //mi += tempValues[1];
                    //dtw += tempValues[2];
                    count++;
                }
                mcCorr = (count == 0) ? 0 : mcCorr/count;
                
                if (corr > 0) {
                    if (mcCorr >= corr)
                        pValueCorr += 1;
                }
                else {
                    if (mcCorr <= corr)
                        pValueCorr += 1;
                }
                if (pValueCorr > (alpha*repetitions)) break; // pruning
            }
            
            pValueCorr = pValueCorr/((double)(repetitions));
            emitKeyValue(key, corr, mi, dtw, pValueCorr, pValueMI, pValueDTW);
            break;
        case FrameworkUtils.CITY:
            for (int j = 0; j < repetitions; j++) {
                double[] tempValues = computeCorrelationTechniques(timeSeries, 0, 0, true);
                double mcCorr = (tempValues == null) ? 0 : tempValues[0];
                double mcMI = (tempValues == null) ? 0 : tempValues[1];
                double mcDTW = (tempValues == null) ? 0 : tempValues[2];
                
                if (corr > 0) {
                    if (mcCorr >= corr)
                        pValueCorr += 1;
                }
                else {
                    if (mcCorr <= corr)
                        pValueCorr += 1;
                }
                
                if (mcMI >= mi)
                    pValueMI += 1;
                
                if (dtw > 0) {
                    if (mcDTW >= dtw)
                        pValueDTW += 1;
                }
                else {
                    if (mcDTW <= dtw)
                        pValueDTW += 1;
                }
            }
            
            pValueCorr = pValueCorr/((double)(repetitions));
            pValueMI = pValueMI/((double)(repetitions));
            pValueDTW = pValueDTW/((double)(repetitions));
            emitKeyValue(key, corr, mi, dtw, pValueCorr, pValueMI, pValueDTW);
            break;
        default:
            // do nothing
            break;
        }
        
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    	out.close();
    }
    
    private void emitKeyValue(PairAttributeWritable key, double corr, double mi, double dtw,
            double pValueCorr, double pValueMI, double pValueDTW) throws IOException, InterruptedException {
        keyWritable = new Text(header.get(dataset1).get(key.getFirstAttribute()) + "," +
                header.get(dataset2).get(key.getSecondAttribute()));
        valueWritable = new Text(corr + "," + mi + "," + dtw + "," + pValueCorr
                + "," + pValueMI + "," + pValueDTW);
        out.write(keyWritable, valueWritable, fileName);
    }
    
    private double[] computeCorrelationTechniques(ArrayList<TreeMap<Integer, Float>>[] timeSeries,
            int index1, int index2, boolean temporalPermutation) {
        double[] values = {0.0, 0.0, 0.0};
        
        TreeMap<Integer, Float> map1 = timeSeries[index1].get(dataset1Key);
        TreeMap<Integer, Float> map2 = timeSeries[index2].get(dataset2Key);
        
        ArrayList<Double> array1 =  new ArrayList<Double>();
        ArrayList<Double> array2 =  new ArrayList<Double>();
            
        for (int temp : map1.keySet()) {
            if (map2.containsKey(temp)) {
                array1.add((double) map1.get(temp));
                array2.add((double) map2.get(temp));
            }
        }
        
        double[] completeTempArray1 =  new double[map1.keySet().size()];
        int index = 0;
        for (int temp : map1.keySet()) {
            completeTempArray1[index] = map1.get(temp);
            index++;
        }
        double[] completeTempArray2 =  new double[map2.keySet().size()];
        index = 0;
        for (int temp : map2.keySet()) {
            completeTempArray2[index] = map2.get(temp);
            index++;
        }
            
        map1.clear();
        map2.clear();
        
        if (array1.size() < 2)
            return null;
        
        // Pearson's Correlation
            
        double[] tempDoubleArray1 = new double[array1.size()];
        double[] tempDoubleArray2 = new double[array2.size()];
        
        int indexD1 = (temporalPermutation) ? new Random().nextInt(array1.size()) : 0;
        int indexD2 = (temporalPermutation) ? new Random().nextInt(array2.size()) : 0;
        for (int i = 0; i < array1.size(); i++) {
            int j = (indexD1 + i) % array1.size();
            int k = (indexD2 + i) % array2.size();
            tempDoubleArray1[i] = array1.get(j);
            tempDoubleArray2[i] = array2.get(k);
        }
        
        array1 = null;
        array2 = null;
        
        PearsonsCorrelation pearsonsCorr = new PearsonsCorrelation();
        values[0] = pearsonsCorr.correlation(tempDoubleArray1, tempDoubleArray2);
        
        // Mutual Information
        
        try {
            values[1] = getMIScore(tempDoubleArray1, tempDoubleArray2);
        } catch (Exception e) {
            e.printStackTrace();
            /*String data1 = "";
            for (double d : tempDoubleArray1)
                data1 += d + ", ";
            String data2 = "";
            for (double d : tempDoubleArray2)
                data2 += d + ", ";
            System.out.println(data1);
            System.out.println(data2);*/
            System.exit(-1);
        }
        
        tempDoubleArray1 = null;
        tempDoubleArray2 = null;
        
        // DTW
        
        double[] completeTempDoubleArray1 = new double[completeTempArray1.length];
        double[] completeTempDoubleArray2 = new double[completeTempArray2.length];
        
        if (temporalPermutation) {
            indexD1 = new Random().nextInt(completeTempArray1.length);
            for (int i = 0; i < completeTempArray1.length; i++) {
                int j = (indexD1 + i) % completeTempArray1.length;
                completeTempDoubleArray1[i] = completeTempArray1[j];
            }
            
            indexD2 = new Random().nextInt(completeTempArray2.length);
            for (int i = 0; i < completeTempArray2.length; i++) {
                int j = (indexD2 + i) % completeTempArray2.length;
                completeTempDoubleArray2[i] = completeTempArray2[j];
            }
        } else {
            System.arraycopy(completeTempArray1, 0, completeTempDoubleArray1, 0,
                    completeTempArray1.length);
            System.arraycopy(completeTempArray2, 0, completeTempDoubleArray2, 0,
                    completeTempArray2.length);
        }
        
        completeTempArray1 = null;
        completeTempArray2 = null;
        
        completeTempDoubleArray1 = normalize(completeTempDoubleArray1);
        completeTempDoubleArray2 = normalize(completeTempDoubleArray2);
        
        values[2] = getDTWScore(completeTempDoubleArray1, completeTempDoubleArray2);
        
        return values;
    }
    
    private double[] normalize(double[] array) {
        DescriptiveStatistics stats = new DescriptiveStatistics(array);
        double mean = stats.getMean();
        double stdDev = stats.getStandardDeviation();
        for (int i = 0; i < array.length; i++) {
            array[i] = (array[i] - mean)/stdDev;
        }
        return array;
    }
    
    private double getMIScore(double[] array1, double[] array2) throws Exception {
        MutualInfoCalculatorMultiVariateKernel mutual =
                new MutualInfoCalculatorMultiVariateKernel();
        mutual.setProperty("NORMALISE_PROP_NAME", "true");
        mutual.initialise(1, 1);
        mutual.setObservations(array1, array2);
        double mi = mutual.computeAverageLocalOfObservations();
        
        EntropyCalculatorKernel entropyCalculator = 
                new EntropyCalculatorKernel();
        entropyCalculator.setProperty("NORMALISE_PROP_NAME", "true");
        entropyCalculator.initialise();
        entropyCalculator.setObservations(array1);
        double entropy1 = entropyCalculator.computeAverageLocalOfObservations();
        
        entropyCalculator.initialise();
        entropyCalculator.setObservations(array2);
        double entropy2 = entropyCalculator.computeAverageLocalOfObservations();
        
        /*double mi = MutualInformation.calculateMutualInformation(
                array1, array2);
        double entropy1 = Entropy.calculateEntropy(array1);
        double entropy2 = Entropy.calculateEntropy(array2);*/
        
        if ((entropy1 == 0) || (entropy2 == 0))
            return 0;
        
        return (mi/Math.sqrt(entropy1*entropy2)); // normalized variant
    }
    
    private double getDTWScore(double[] array1, double[] array2) {
        double[] constantArray1 = new double[array1.length];
        Arrays.fill(constantArray1, 0);
        double[] constantArray2 = new double[array2.length];
        Arrays.fill(constantArray2, 0);
        
        double dtwAB = DTW.getWarpDistBetween(
                new TimeSeries(new DenseInstance(array1)),
                new TimeSeries(new DenseInstance(array2)));
        double dtwA0 = DTW.getWarpDistBetween(
                new TimeSeries(new DenseInstance(array1)),
                new TimeSeries(new DenseInstance(constantArray2)));
        double dtwB0 = DTW.getWarpDistBetween(
                new TimeSeries(new DenseInstance(array2)),
                new TimeSeries(new DenseInstance(constantArray1)));
        
        if ((dtwA0 + dtwB0) == 0)
            return 0;
        
        return (1 - (dtwAB/(dtwA0 + dtwB0)));
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
    private ArrayList<Integer[]> bfsShift(boolean isNbhd) {
        if (isNbhd)
            return spatialGraph.generateRandomShift();
        return zipGraph.generateRandomShift();
    }
}
