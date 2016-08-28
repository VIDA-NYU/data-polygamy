/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.exp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.nyu.vida.data_polygamy.utils.Utilities;

public class CorrelationGraph {
    
    public final static int PEARSON = 1;
    public final static int MUTUAL = 2;
    public final static int DTW = 3;
    
    public String temporalRes = "";
    public String spatialRes = "";
    
    String aggregatesPatternStr = "([a-zA-Z0-9]+[\\-]?[a-zA-Z0-9]+){1}\\-[a-z]+\\-[a-z]+\\.[a-z]+";
    String correlationPatternStr = "([a-zA-Z0-9]+[\\-]?[a-zA-Z0-9]+){1}\\-([a-zA-Z0-9]+" +
            "[\\-]?[a-zA-Z0-9]+){1}\\-([a-z]+)\\-([a-z]+)";
    Pattern aggregatesPattern = Pattern.compile(aggregatesPatternStr);
    Pattern correlationPattern = Pattern.compile(correlationPatternStr);
    
    public class DataSet {
        public int id;
        public String name;
        public int nAttributes = 0;
        public int nFiles = 0;
        public boolean hasData = false;
        
        public HashSet<Integer> attributes = new HashSet<Integer>();
        public HashSet<Integer> adjacent = new HashSet<Integer>();
    }
    
    public class Attribute {
        public int id;
        public String dataset;
        public String name;
        
        public HashSet<Integer> adjacent = new HashSet<Integer>();
    }
    
    public class Edge {
        int a1;
        int a2;
        
        int d1;
        int d2;
        
        double pearsonCorr;
        double mi;
        double dtw;
        
        double pValueCorr;
        double pValueMI;
        double pValueDTW;
    }
    
    HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    HashMap<String, Integer> attributeMap = new HashMap<String, Integer>();
    
    ArrayList<DataSet> datasets = new ArrayList<DataSet>();
    ArrayList<Attribute> attributes = new ArrayList<Attribute>();
    
    ArrayList<Edge> edges = new ArrayList<Edge>();

    public void createGraph(String dataFolder, String metadataFolder,
            String temporalRes, String spatialRes) throws IOException {
        
        this.temporalRes = temporalRes;
        this.spatialRes = spatialRes;
        
        File fol = new File(dataFolder);
        
        FilenameFilter fileNameFilter = new FilenameFilter() {
            
            @Override
            public boolean accept(File dir, String name) {
                if (name.contains(".crc"))
                    return false;
                return true;
            }
        };
        
        File[] allFiles = fol.listFiles(fileNameFilter);
        for(File file: allFiles) {
            String fileName = file.getName().replace("gas-prices", "gasprices");
            System.out.println(fileName);
            
            Matcher m = correlationPattern.matcher(fileName);
            if (!m.find()) continue;
            
            String ds1 = m.group(1);
            String ds2 = m.group(2);
            
            String temporalResolution = m.group(3);
            String spatialResolution = m.group(4);
            
            DataSet d1 = getDataSet(ds1);
            DataSet d2 = getDataSet(ds2);
            
            d1.nFiles++;
            d2.nFiles++;
            
            if (temporalResolution.equals(temporalRes) &&
                    spatialResolution.equals(spatialRes)) {
                d1.hasData = true;
                d2.hasData = true;
                
                BufferedReader reader = new BufferedReader(new FileReader(file));
                while(readPair(reader,d1,d2));
            }
        }
        
        // Metadata.
        
        FilenameFilter metadataFilter = new FilenameFilter() {
            
            @Override
            public boolean accept(File dir, String name) {
               if (name.contains(".aggregates"))
                  return true;
               return false;
            }
        };
        
        fol = new File(metadataFolder);
        File[] metadataFiles = fol.listFiles(metadataFilter);
        for(File file: metadataFiles) {
            String fileName = file.getName().replace("gas-prices", "gasprices");
            
            Matcher m = aggregatesPattern.matcher(fileName);
            if (!m.find()) continue;
            String dataset = m.group(1);
            
            if (!datasetExists(dataset))
                continue;
            DataSet d = getDataSet(dataset);
            
            System.out.println(dataset);
            
            BufferedReader reader = new BufferedReader(new FileReader(file));
            reader.readLine();
            int count = Integer.parseInt(reader.readLine());
            d.nAttributes = count;
            reader.close();
         }
        
    }
    
    private String replace(String string) {
        return string.replace(' ', '_').replace('.', '_').replace('[', '_').replace(']', '_').replace('-', '_');
    }

    private boolean readPair(BufferedReader reader, DataSet d1, DataSet d2) throws IOException {
        String [] a = Utilities.getLine(reader, "\t");
        
        if(a == null) {
            return false;
        }
        
        String [] attributes = a[0].split(",");
        String [] values = a[1].split(",");
        
        Edge e = new Edge();
        e.pearsonCorr = Double.parseDouble(values[0]);
        e.mi = Double.parseDouble(values[1]);
        e.dtw = Double.parseDouble(values[2]);
        e.pValueCorr = Double.parseDouble(values[3]);
        e.pValueMI = Double.parseDouble(values[4]);
        e.pValueDTW = Double.parseDouble(values[5]);
        
        Attribute a1 = getAttribute(d1,attributes[0]);
        Attribute a2 = getAttribute(d2,attributes[1]);
            
        e.a1 = a1.id;
        e.a2 = a2.id;
        
        e.d1 = d1.id;
        e.d2 = d2.id;
            
        int ein = edges.size();
        edges.add(e);
        a1.adjacent.add(ein);
        a2.adjacent.add(ein);
        
        d1.adjacent.add(d2.id);
        d2.adjacent.add(d1.id);
        
        return true;
    }

    private Attribute getAttribute(DataSet d, String aName) {
        String s = d.name + "." + aName;
        Integer ain = attributeMap.get(s);
        if(ain == null) {
            Attribute a = new Attribute();
            a.name = aName;
            a.dataset = d.name;
            ain = attributes.size();
            a.id = ain;
            
            attributeMap.put(s,ain);
            attributes.add(a);
            d.attributes.add(ain);
        }
        return attributes.get(ain);
    }
    
    private boolean datasetExists(String name) {
        if (dataMap.get(name) == null)
            return false;
        return true;
    }

    private DataSet getDataSet(String name) {
        Integer din = dataMap.get(name);
        if(din == null) {
            DataSet d = new DataSet();
            d.name = name;
            din = datasets.size();
            d.id = din;
            
            dataMap.put(name,din);
            datasets.add(d);
        }
        return datasets.get(din);
    }

    public void printMetaGraph(String outputDir, String threshold,
            int type) throws IOException {
        
        int totalNDatasets = dataMap.size();
        if (totalNDatasets < 2) return;
        
        double th = Double.parseDouble(threshold);
        
        class DataSetComparator implements Comparator<DataSet> {
            @Override
            public int compare(DataSet o1, DataSet o2) {
                return o1.nAttributes - o2.nAttributes;
            }
        }
        
        ArrayList<DataSet> sortedList = new ArrayList<DataSet>(datasets);
        Collections.sort(sortedList, new DataSetComparator());
        
        for (int nDatasets = 2; nDatasets < totalNDatasets + 1; ++nDatasets) {
        
            HashSet<DataSet> finalDataSets = new HashSet<DataSet>();
            HashSet<String> finalDataSetsIds = new HashSet<String>();
            for (DataSet d : sortedList) {
                if (!d.hasData) continue;
                finalDataSets.add(d);
                finalDataSetsIds.add(d.name);
                System.out.println("Dataset: " + d.name);
                if (finalDataSets.size() == nDatasets) break;
            }
            
            System.out.println("");
            
            if (finalDataSets.size() != nDatasets) continue;
            
            int maxEdges = 0;
            int nAttributes = 0;
            int numberSignificant = 0;
            
            for (DataSet d1 : finalDataSets) {
                nAttributes += d1.nAttributes;
                for (DataSet d2 : finalDataSets) {
                    if (d1.id == d2.id) continue;
                    maxEdges += d1.nAttributes * d2.nAttributes;
                }
            }
            maxEdges /= 2;
            
            ArrayList<Edge> finalEdges = new ArrayList<Edge>();
            for(Edge e: edges) {
                Attribute a1 = attributes.get(e.a1);
                Attribute a2 = attributes.get(e.a2);
                
                if (!finalDataSetsIds.contains(a1.dataset)) continue;
                if (!finalDataSetsIds.contains(a2.dataset)) continue;
                
                switch(type) {
                case PEARSON:
                    if(e.pValueCorr <= 0.05) {
                        numberSignificant++;
                        if(Math.abs(e.pearsonCorr) >= th) {
                            finalEdges.add(e);
                        }
                    }
                    break;
                case MUTUAL:
                    if(e.pValueMI <= 0.05) {
                        numberSignificant++;
                        if(e.mi >= th) {
                            finalEdges.add(e);
                        }
                    }
                    break;
                case DTW:
                    if(e.pValueDTW <= 0.05) {
                        numberSignificant++;
                        if(Math.abs(e.dtw) >= th) {
                            finalEdges.add(e);
                        }
                    }
                    break;
                default:
                    if(e.pValueCorr <= 0.05) {
                        numberSignificant++;
                        if(Math.abs(e.pearsonCorr) >= th) {
                            finalEdges.add(e);
                        }
                    }
                    break;
                }
            }
            
            String name = "";
            switch(type) {
            case PEARSON:
                name = "pearson";
                break;
            case MUTUAL:
                name = "mi";
                break;
            case DTW:
                name = "dtw";
                break;
            default:
                name = "pearson";
                break;
            }
            
            File outputFile = new File(outputDir, name + "-" + th +
                    "-" + this.temporalRes + "-" + this.spatialRes + "-" + nDatasets + ".out");
            
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));
            String result = "";
            
            result += "datasets: " + nDatasets + "\n";
            result += "attributes: " + nAttributes + "\n";
            result += "max edges: " + maxEdges + "\n";
            result += "significant edges: " + numberSignificant + "\n";
            result += "  score >= " + th + ": " + finalEdges.size() + "\n\n";
            
            result += "graph relationship {\n";
            String [] color = {"red","blue"};
            
            for(Edge e: finalEdges) {
                Attribute a1 = attributes.get(e.a1);
                Attribute a2 = attributes.get(e.a2);
                int c = 0;
                
                double score = 0.0;
                switch(type) {
                case PEARSON:
                    score = e.pearsonCorr;
                    break;
                case MUTUAL:
                    score = e.mi;
                    break;
                case DTW:
                    score = e.dtw;
                    break;
                default:
                    score = e.pearsonCorr;
                    break;
                }
                
                result += replace(a1.dataset) + "_" + replace(a1.name) + " -- " + replace(a2.dataset) + "_" + replace(a2.name) +
                        " (score=" + score + ") [color=" + color[c] + "]" + ";\n";
            }
            result += "}\n";
            
            bw.write(result);
            bw.close();
        }
    }

}
