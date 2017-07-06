/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.utils;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Months;
import org.joda.time.MutableDateTime;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

import edu.nyu.vida.data_polygamy.scalar_function.Aggregation;
import edu.nyu.vida.data_polygamy.scalar_function.Average;
import edu.nyu.vida.data_polygamy.scalar_function.Count;
import edu.nyu.vida.data_polygamy.scalar_function.CountGradient;
import edu.nyu.vida.data_polygamy.scalar_function.Gradient;
import edu.nyu.vida.data_polygamy.scalar_function.Max;
import edu.nyu.vida.data_polygamy.scalar_function.Median;
import edu.nyu.vida.data_polygamy.scalar_function.Min;
import edu.nyu.vida.data_polygamy.scalar_function.Mode;
import edu.nyu.vida.data_polygamy.scalar_function.Sum;
import edu.nyu.vida.data_polygamy.scalar_function.Unique;

public class FrameworkUtils {
    
    /**
     * Resolutions
     */
    
    // spatial resolutions
    public final static int CITY = 6;
    public final static int GRID = 5;
    public final static int ZIP = 4;
    public final static int NBHD = 3;
    public final static int BLOCK = 2;
    public final static int BBL = 1;
    public final static int POINTS = 0;
    
    // temporal resolutions
    public final static int YEAR = 4;
    public final static int MONTH = 3;
    public final static int WEEK = 2;
    public final static int DAY = 1;
    public final static int HOUR = 0;
    
    // maps
    public HashMap<String,Integer> spatialRes = new HashMap<String,Integer>();
    public HashMap<Integer,String> spatialResStr = new HashMap<Integer,String>();
    public HashMap<String,Integer> temporalRes = new HashMap<String,Integer>();
    public HashMap<Integer,String> temporalResStr = new HashMap<Integer,String>();
    
    public FrameworkUtils() {
        
        // spatial resolution
        spatialRes.put("bbl", BBL);
        spatialRes.put("block", BLOCK);
        spatialRes.put("city", CITY);
        spatialRes.put("nbhd", NBHD);
        spatialRes.put("zip", ZIP);
        spatialRes.put("grid", GRID);
        spatialRes.put("points", POINTS);
        
        spatialResStr.put(BBL, "bbl");
        spatialResStr.put(BLOCK, "block");
        spatialResStr.put(CITY, "city");
        spatialResStr.put(NBHD, "nbhd");
        spatialResStr.put(ZIP, "zip");
        spatialResStr.put(GRID, "grid");
        spatialResStr.put(POINTS, "points");
        
        // temporal resolution
        temporalRes.put("year", YEAR);
        temporalRes.put("month", MONTH);
        temporalRes.put("week", WEEK);
        temporalRes.put("day", DAY);
        temporalRes.put("hour", HOUR);
        
        temporalResStr.put(YEAR, "year");
        temporalResStr.put(MONTH, "month");
        temporalResStr.put(WEEK, "week");
        temporalResStr.put(DAY, "day");
        temporalResStr.put(HOUR, "hour");
    }
    
    public String[] getSpatialResolutions() {
        String[] result = {
                "bbl",
                "block",
                "city",
                "nbhd",
                "zip",
                "grid",
                "points"};
        return result;
    }

    public int spatialResolution(String res) {
        if (res.contains("grid"))
            res = "grid";
        Integer result = spatialRes.get(res.toLowerCase());
        
        if (result == null)
            return -1;
        
        return result;
    }
    
    public int temporalResolution(String res) {
        Integer result = temporalRes.get(res.toLowerCase());
        
        if (result == null)
            return -1;
        
        return result;
    }
    
    public String spatialResolutionStr(int spatialResolution) {
    	return spatialResStr.get(spatialResolution);
    }
    
    public String temporalResolutionStr(int temporalResolution) {
    	return temporalResStr.get(temporalResolution);
    }
    
    public static String[] getAggTempResolutions(int temporalResolution) {
        String[] result = null;
        
        switch(temporalResolution) {
        
        case FrameworkUtils.HOUR:
            result = new String[5];
            result[0] = "hour";
            result[1] = "day";
            result[2] = "week";
            result[3] = "month";
            result[4] = "year";
            break;
        case FrameworkUtils.DAY:
            result = new String[4];
            result[0] = "day";
            result[1] = "week";
            result[2] = "month";
            result[3] = "year";
            break;
        case FrameworkUtils.WEEK:
            result = new String[3];
            result[0] = "week";
            result[1] = "month";
            result[2] = "year";
            break;
        case FrameworkUtils.MONTH:
            result = new String[2];
            result[0] = "month";
            result[1] = "year";
            break;
        case FrameworkUtils.YEAR:
            result = new String[1];
            result[0] = "year";
            break;
        default:
            result = new String[1];
            result[0] = "year";
            break;
        }
        
        return result;
    }
    
    public static String[] getAggSpatialResolutions(int spatialResolution) {
        String[] result = null;
        
        switch(spatialResolution) {
        
        case FrameworkUtils.POINTS:
            result = new String[5];
            result[0] = "nbhd";
            result[1] = "zip";
            result[2] = "block";
            result[3] = "bbl";
            result[4] = "city";
            break;
        case FrameworkUtils.BBL:
            result = new String[4];
            result[0] = "nbhd";
            result[1] = "zip";
            result[2] = "block";
            result[3] = "city";
            break;
        case FrameworkUtils.NBHD:
            result = new String[2];
            result[0] = "nbhd";
            result[1] = "city";
            break;
        case FrameworkUtils.ZIP:
            result = new String[2];
            result[0] = "zip";
            result[1] = "city";
            break;
        case FrameworkUtils.BLOCK:
            result = new String[1];
            result[0] = "block";
            break;
        case FrameworkUtils.CITY:
            result = new String[1];
            result[0] = "city";
            break;
        default:
            result = new String[1];
            result[0] = "city";
            break;
        }
        
        return result;
    }
    
    public static String getTemporalFormat(int temporalResolution) {
        
        String format = "";
        
        switch (temporalResolution) {
        
        case YEAR:
            format = "yyyy z";
            break;
        case MONTH:
            format = "yyyy-MM z";
            break;
        case WEEK:
            format = "yyyy-MM-ww z";
            break;
        case DAY:
            format = "yyyy-MM-dd z";
            break;
        case HOUR:
            format = "yyyy-MM-dd-HH z";
            break;
        default:
            format = "yyyy-MM-dd-HH z";
            break;
        }
        
        return format;
    }
    
    public static String getTemporalStr(int temporalResolution, int time) {
        
        DateTime dt = new DateTime(((long)time)*1000, DateTimeZone.UTC);
        String temporal = "";
        
        switch (temporalResolution) {
        
        case HOUR:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString() + "-" +
                    dt.dayOfMonth().getAsString() + "-" +
                    dt.hourOfDay().getAsString();
            break;
        case DAY:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString() + "-" +
                    dt.dayOfMonth().getAsString();
            break;
        case WEEK:
            temporal = dt.weekyear().getAsString() + "-" +
                    dt.weekOfWeekyear().getAsString();
            break;
        case MONTH:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString();
            break;
        case YEAR:
            temporal = dt.year().getAsString();
            break;
        default:
            temporal = null;
            break;
        }
        
        return temporal;
    }
    
    public static String getTemporalStr(int temporalResolution, DateTime dt) {

        String temporal = "";
        
        switch (temporalResolution) {
        
        case HOUR:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString() + "-" +
                    dt.dayOfMonth().getAsString() + "-" +
                    dt.hourOfDay().getAsString();
            break;
        case DAY:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString() + "-" +
                    dt.dayOfMonth().getAsString();
            break;
        case WEEK:
            temporal = dt.weekyear().getAsString() + "-" +
                    dt.weekOfWeekyear().getAsString();
            break;
        case MONTH:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString();
            break;
        case YEAR:
            temporal = dt.year().getAsString();
            break;
        default:
            temporal = null;
            break;
        }
        
        return temporal;
    }
    
    public static int getTime(String[] input, int tempPosition) {
        
        int time = -1;
        
        try {
            time = (int) Double.parseDouble(input[tempPosition]);
        } catch (Exception e) {
            return -1;
        }
        
        if (time < 0)
            return -1;
        
        return time;
    }
    
    public static int getTime(int temporalResolution, String[] input, int tempPosition) {
        
        String temporal = "";
        String format  = "";
        
        long time = 0L;
        
        try {
            time = ((long)Double.parseDouble(input[tempPosition]))*1000;
        } catch (Exception e) {
            return -1;
        }
        
        if (time < 0)
            return -1;
        
        DateTime dt = new DateTime(time, DateTimeZone.UTC);
        
        switch (temporalResolution) {
        
        case HOUR:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString() + "-" +
                    dt.dayOfMonth().getAsString() + "-" +
                    dt.hourOfDay().getAsString();
            format = "yyyy-MM-dd-HH z";
            break;
        case DAY:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString() + "-" +
                    dt.dayOfMonth().getAsString();
            format = "yyyy-MM-dd z";
            break;
        case WEEK:
            temporal = dt.weekyear().getAsString() + "-" +
                    dt.weekOfWeekyear().getAsString();
            format = "xxxx-ww z";
            break;
        case MONTH:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString();
            format = "yyyy-MM z";
            break;
        case YEAR:
            temporal = dt.year().getAsString();
            format = "yyyy z";
            break;
        default:
            temporal = null;
            format = "";
            break;
        }
        
        if (temporal == null)
            return -1;
        
        DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
        return (int) (formatter.parseDateTime(temporal + " UTC").getMillis()/1000);
    }
    
    public static int getTime(int temporalResolution, int[] input, int tempPosition) {
        
        String temporal = "";
        String format  = "";
        
        long time = 0L;
        
        try {
            time = ((long)input[tempPosition])*1000;
        } catch (Exception e) {
            return -1;
        }
        
        if (time < 0)
            return -1;
        
        DateTime dt = new DateTime(time, DateTimeZone.UTC);
        
        switch (temporalResolution) {
        
        case HOUR:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString() + "-" +
                    dt.dayOfMonth().getAsString() + "-" +
                    dt.hourOfDay().getAsString();
            format = "yyyy-MM-dd-HH z";
            break;
        case DAY:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString() + "-" +
                    dt.dayOfMonth().getAsString();
            format = "yyyy-MM-dd z";
            break;
        case WEEK:
            temporal = dt.weekyear().getAsString() + "-" +
                    dt.weekOfWeekyear().getAsString();
            format = "xxxx-ww z";
            break;
        case MONTH:
            temporal = dt.year().getAsString() + "-" +
                    dt.monthOfYear().getAsString();
            format = "yyyy-MM z";
            break;
        case YEAR:
            temporal = dt.year().getAsString();
            format = "yyyy z";
            break;
        default:
            temporal = null;
            format = "";
            break;
        }
        
        if (temporal == null)
            return -1;
        
        DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
        return (int) (formatter.parseDateTime(temporal + " UTC").getMillis()/1000);
    }
    
    public static int getTimeSteps(int tempRes, int startTime, int endTime) {
        
        if (startTime > endTime) {
            return 0;
        }
        
        int timeSteps = 0;
        DateTime start = new DateTime(((long)startTime)*1000, DateTimeZone.UTC);
        DateTime end = new DateTime(((long)endTime)*1000, DateTimeZone.UTC);
        
        switch(tempRes) {
        case FrameworkUtils.HOUR:
            timeSteps = Hours.hoursBetween(start, end).getHours();
            break;
        case FrameworkUtils.DAY:
            timeSteps = Days.daysBetween(start, end).getDays();
            break;
        case FrameworkUtils.WEEK:
            timeSteps = Weeks.weeksBetween(start, end).getWeeks();
            break;
        case FrameworkUtils.MONTH:
            timeSteps = Months.monthsBetween(start, end).getMonths();
            break;
        case FrameworkUtils.YEAR:
            timeSteps = Years.yearsBetween(start, end).getYears();
            break;
        default:
            timeSteps = Hours.hoursBetween(start, end).getHours();
            break;
        }
        timeSteps++;
        
        return timeSteps;
    }
    
    public static int addTimeSteps(int tempRes, int increment, DateTime start) {
        
        DateTime d;
        
        switch(tempRes) {
        case FrameworkUtils.HOUR:
            d = start.plusHours(increment);
            break;
        case FrameworkUtils.DAY:
            d = start.plusDays(increment);
            break;
        case FrameworkUtils.WEEK:
            d = start.plusWeeks(increment);
            break;
        case FrameworkUtils.MONTH:
            d = start.plusMonths(increment);
            break;
        case FrameworkUtils.YEAR:
            d = start.plusYears(increment);
            break;
        default:
            d = start.plusHours(increment);
            break;
        }
        
        return (int) (d.getMillis()/1000);
        
    }
    
    public static DateTime addTime(int tempRes, int increment, DateTime start) {
        
        DateTime d = null;
        
        switch(tempRes) {
        case FrameworkUtils.HOUR:
            d = start.plusHours(increment);
            break;
        case FrameworkUtils.DAY:
            d = start.plusDays(increment);
            break;
        case FrameworkUtils.WEEK:
            d = start.plusWeeks(increment);
            break;
        case FrameworkUtils.MONTH:
            d = start.plusMonths(increment);
            break;
        case FrameworkUtils.YEAR:
            d = start.plusYears(increment);
            break;
        default:
            d = start.plusHours(increment);
            break;
        }
        
        return d;
        
    }
    
    public static int getDeltaSinceEpoch(int time, int tempRes) {
        int delta = 0;
        
        // Epoch
        MutableDateTime epoch = new MutableDateTime();
        epoch.setDate(0);
        
        DateTime dt = new DateTime(time*1000, DateTimeZone.UTC);
        
        switch(tempRes) {
        case FrameworkUtils.HOUR:
            Hours hours = Hours.hoursBetween(epoch, dt);
            delta = hours.getHours();
            break;
        case FrameworkUtils.DAY:
            Days days = Days.daysBetween(epoch, dt);
            delta = days.getDays();
            break;
        case FrameworkUtils.WEEK:
            Weeks weeks = Weeks.weeksBetween(epoch, dt);
            delta = weeks.getWeeks();
            break;
        case FrameworkUtils.MONTH:
            Months months = Months.monthsBetween(epoch, dt);
            delta = months.getMonths();
            break;
        case FrameworkUtils.YEAR:
            Years years = Years.yearsBetween(epoch, dt);
            delta = years.getYears();
            break;
        default:
            hours = Hours.hoursBetween(epoch, dt);
            delta = hours.getHours();
            break;
        }
        
        return delta;
    }
    
    /**
     * Aggregations
     */
    
    public static enum Function {
        NONE, AVERAGE, COUNT, SUM,
        MAX, MIN, MEDIAN, MODE, UNIQUE,
        GRADIENT, COUNT_GRADIENT
    }
    
    private static final Function[] functions = Function.values();
    
    public static Aggregation getAggregation(Function function) {
        
        switch (function) {
        case AVERAGE:
            return new Average();
        case COUNT:
            return new Count();
        case MAX:
            return new Max();
        case MIN:
            return new Min();
        case SUM:
            return new Sum();
        case MEDIAN:
            return new Median();
        case MODE:
            return new Mode();
        case UNIQUE:
            return new Unique();
        case GRADIENT:
            return new Gradient();
        case COUNT_GRADIENT:
            return new CountGradient();
        default:
            return null;
        }
    }
    
    public static Aggregation getAggregation(int id) {
        Function function = functions[id];
        
        switch (function) {
        case AVERAGE:
            return new Average();
        case COUNT:
            return new Count();
        case MAX:
            return new Max();
        case MIN:
            return new Min();
        case SUM:
            return new Sum();
        case MEDIAN:
            return new Median();
        case MODE:
            return new Mode();
        case UNIQUE:
            return new Unique();
        case GRADIENT:
            return new Gradient();
        case COUNT_GRADIENT:
            return new CountGradient();
        default:
            return null;
        }
    }
    
    public static String functionToString(Function stats) {
        
        String strResult = "";
        switch (stats) {
        
        case AVERAGE:
            strResult = "avg";
            break;
        case COUNT:
            strResult = "count";
            break;
        case MAX:
            strResult = "max";
            break;
        case MIN:
            strResult = "min";
            break;
        case SUM:
            strResult = "sum";
            break;
        case MEDIAN:
            strResult = "median";
            break;
        case MODE:
            strResult = "mode";
            break;
        case UNIQUE:
            strResult = "unique";
            break;
        case GRADIENT:
            strResult = "gradient";
            break;
        case COUNT_GRADIENT:
            strResult = "count_gradient";
            break;
        default:
            strResult = "none";
            break;
        }
        
        return strResult;
    }
    
    /**
     * Type Checking  
     */
    
    public static boolean isNumeric(String val) {
        try {
            Float.parseFloat(val);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
    
    /**
     * Vector
     */
    
    public static class Vector {
        
        private float x1, x2;
        private float y1, y2;
        
        public Vector(float x1, float y1, float x2, float y2) {
            this.x1 = x1;
            this.x2 = x2;
            this.y1 = y1;
            this.y2 = y2;
        }
        
        public float getMagnitude() {
            float squareX = (float) Math.pow(x2 - x1, 2);
            float squareY = (float) Math.pow(y2 - y1, 2);
            return (float) Math.sqrt(squareX + squareY);
        }
        
        public float getDirection() {
            return (float) Math.atan2(y2 - y1, x2 - x1);
        }
        
        public float getX1() {
            return x1;
        }

        public float getX2() {
            return x2;
        }

        public float getY1() {
            return y1;
        }

        public float getY2() {
            return y2;
        }

        public void readFields(DataInput in) throws IOException {
            this.x1 = in.readFloat();
            this.x2 = in.readFloat();
            this.y1 = in.readFloat();
            this.y2 = in.readFloat();
        }

        public void write(DataOutput out) throws IOException {
            out.writeFloat(this.x1);
            out.writeFloat(this.x2);
            out.writeFloat(this.y1);
            out.writeFloat(this.y2);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(this.x1, this.x2, this.y1, this.y2);
        }
        
        public int compareTo(Vector arg0) {
            Vector agg = (Vector) arg0;
            return ComparisonChain.start().
                    compare(this.x1, agg.getX1()).
                    compare(this.x2, agg.getX2()).
                    compare(this.y1, agg.getY1()).
                    compare(this.y2, agg.getY2()).
                    result();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Vector))
                return false;
            return (this.compareTo((Vector) o) == 0) ? true : false;
        }
    }
    
    /**
     * String Parsing 
     */
    
    public static String[] splitStr(String val, Integer len) throws IOException {
        
        String[] input;
        
        try {
            CSVParser parser = new CSVParser(new StringReader(val), CSVFormat.DEFAULT);
            CSVRecord record = parser.getRecords().get(0);
            input = new String[len];
            Iterator<String> valuesIt = record.iterator();
            int i = 0;
            while (valuesIt.hasNext()) {
                input[i] = valuesIt.next().trim();
                i++;
            }
            parser.close();
        } catch (ArrayIndexOutOfBoundsException e) {
            input = val.split(",", len);
            for (int i = 0; i < input.length; i++)
                input[i] = input[i].trim();
        }
        
        return input;
    }
    
    public static String[] splitStr(String val) throws IOException {
        
        CSVParser parser = new CSVParser(new StringReader(val), CSVFormat.DEFAULT);
        CSVRecord record = parser.getRecords().get(0);
        Iterator<String> valuesIt = record.iterator();
        String[] input = new String[record.size()];
        int i = 0;
        while (valuesIt.hasNext()) {
            input[i] = valuesIt.next();
            i++;
        }
        parser.close();
        return input;
    }
    
    public static int[] getIntArray(String[] input) {
        int[] result = new int[input.length];
        for (int i = 0; i < input.length; i++)
            result[i] = Integer.parseInt(input[i]);
        return result;
    }
    
    
    /**
     * This function is used to split a String into a set of tokens seperated by
     * a delimiter
     * 
     * @param s
     * @param delimiter
     * @return The array of tokens
     * 
     * From Harish.
     */
    public static String[] splitString(String s, String delimiter) {
        String[] ret = null;
        StringTokenizer tok;

        if (delimiter == null || delimiter.length() == 0) {
            tok = new StringTokenizer(s);
        } else {
            tok = new StringTokenizer(s, delimiter);
        }

        ret = new String[tok.countTokens()];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = tok.nextToken();
        }
        return ret;
    }
    
    public static String[] getLine(BufferedReader reader, String delimiter) throws IOException {
        String l = reader.readLine();
        if(l == null) {
            return null;
        }
        return splitString(l,delimiter);
    }
    
    /**
     * Writables  
     */
    
    public static class MultipleSpatioTemporalWritable implements WritableComparable<MultipleSpatioTemporalWritable> {

        private int[] spatial;
        private int[] temporal;
        
        // for compareTo method
        private int[] spatialComp;
        int[] temporalComp;
        ComparisonChain ch;
        
        public MultipleSpatioTemporalWritable() {
            this.spatial = new int[1];
            this.temporal = new int[1];
        }
        
        public MultipleSpatioTemporalWritable(ArrayList<Integer> spatial,
                ArrayList<Integer> temporal) {
        	this.spatial = new int[spatial.size()];
        	this.temporal = new int[temporal.size()];
        	int n = Math.max(this.spatial.length, this.temporal.length);
        	for(int i = 0;i < n;i ++) {
        		if(i < this.spatial.length) {
        			this.spatial[i] = spatial.get(i);
        		}
        		if(i < this.temporal.length) {
        			this.temporal[i] = temporal.get(i);
        		}
        	}
        }
        
        public int[] getSpatial() {
            return spatial;
        }
        
        public int[] getTemporal() {
            return temporal;
        }
        
        @Override
        public String toString() {
            String result = "";
            for (int i = 0; i < this.spatial.length; i++)
                result += this.spatial[i] + ",";
            for (int i = 0; i < this.temporal.length; i++)
                result += this.temporal[i] + ",";
            return result.substring(0, result.length()-1);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // spatial
            int size = in.readInt();
            this.spatial = new int[size];
            for (int i = 0; i < size; i++)
                this.spatial[i] = in.readInt();
            
            // temporal
            size = in.readInt();
            this.temporal = new int[size];
            for (int i = 0; i < size; i++)
                this.temporal[i] = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // spatial
            out.writeInt(this.spatial.length);
            for (int i = 0; i < this.spatial.length; i++)
                out.writeInt(this.spatial[i]);
            
            // temporal
            out.writeInt(this.temporal.length);
            for (int i = 0; i < this.temporal.length; i++)
                out.writeInt(this.temporal[i]);
        }

        @Override
        // TODO: is this efficient enough? It seems slow
        public int compareTo(MultipleSpatioTemporalWritable arg0) {
            spatialComp = arg0.getSpatial();
            temporalComp = arg0.getTemporal();
            ch = ComparisonChain.start();
            
            // assuming they have the same size
            for (int i = 0; i < this.spatial.length; i++)
                ch = ch.compare(this.spatial[i], spatialComp[i]);

            for (int i = 0; i < this.temporal.length; i++)
                ch = ch.compare(this.temporal[i], temporalComp[i]);
            
            return ch.result();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(this.spatial, this.temporal);
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof MultipleSpatioTemporalWritable))
                return false;
            return (this.compareTo((MultipleSpatioTemporalWritable) o) == 0) ? true : false;
        }

    }
    
    public static class SpatioTemporalWritable implements WritableComparable<SpatioTemporalWritable> {

        private int spatial;
        private int temporal;
        private int spatialResolution;
        private int temporalResolution;
        private int dataset;
        
        public SpatioTemporalWritable() {
            this.spatial = -1;
            this.temporal = -1;
            this.spatialResolution = -1;
            this.temporalResolution = -1;
            this.dataset = -1;
        }
        
        public SpatioTemporalWritable(int spatial, int temporal,
        		int spatialResolution, int temporalResolution,
        		int dataset) {
            this.spatial = spatial;
            this.temporal = temporal;
            this.spatialResolution = spatialResolution;
            this.temporalResolution = temporalResolution;
            this.dataset = dataset;
        }
        
        public int getSpatial() {
            return spatial;
        }
        
        public int getTemporal() {
            return temporal;
        }
        
        public int getSpatialResolution() {
        	return spatialResolution;
        }
        
        public int getTemporalResolution() {
        	return temporalResolution;
        }
        
        public int getDataset() {
            return dataset;
        }
        
        @Override
        public String toString() {
            return String.valueOf(temporal) + "," + String.valueOf(spatial) +
            		"," + String.valueOf(dataset) + "," +
            		String.valueOf(temporalResolution) + "," +
            		String.valueOf(spatialResolution);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // spatial
            this.spatial = in.readInt();
            
            // temporal
            this.temporal = in.readInt();
            
            // spatial resolution
            this.spatialResolution = in.readInt();
            
            // temporal resolution
            this.temporalResolution = in.readInt();
            
            // dataset
            this.dataset = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // spatial
            out.writeInt(this.spatial);
                        
            // temporal
            out.writeInt(this.temporal);
            
            // spatial resolution
            out.writeInt(this.spatialResolution);
            
            // temporal resolution
            out.writeInt(this.temporalResolution);
            
            // dataset
            out.writeInt(this.dataset);
        }

        @Override
        public int compareTo(SpatioTemporalWritable arg0) {
            return ComparisonChain.start()
                    .compare(this.spatial, arg0.getSpatial())
                    .compare(this.temporal, arg0.getTemporal())
                    .compare(this.spatialResolution, arg0.getSpatialResolution())
                    .compare(this.temporalResolution, arg0.getTemporalResolution())
                    .compare(this.dataset, arg0.getDataset())
                    .result();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(this.spatial, this.temporal,
            		this.spatialResolution, this.temporalResolution,
            		this.dataset);
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SpatioTemporalWritable))
                return false;
            return (this.compareTo((SpatioTemporalWritable) o) == 0) ? true : false;
        }

    }
    
    public static class SpatioAttributeWritable implements WritableComparable<SpatioAttributeWritable> {

        private int spatial;
        private int attribute;
        
        public SpatioAttributeWritable() {
            this.spatial = -1;
            this.attribute = -1;
        }
        
        public SpatioAttributeWritable(int spatial, int attribute) {
            this.spatial = spatial;
            this.attribute = attribute;
        }
        
        public int getSpatial() {
            return spatial;
        }
        
        public int getAttribute() {
            return attribute;
        }
        
        @Override
        public String toString() {
            return String.valueOf(spatial) + "," + String.valueOf(attribute);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // spatial
            this.spatial = in.readInt();
            
            // attribute
            this.attribute = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // spatial
            out.writeInt(this.spatial);
            
            // attribute
            out.writeInt(this.attribute);
        }

        @Override
        public int compareTo(SpatioAttributeWritable arg0) {
            return ComparisonChain.start()
                    .compare(this.spatial, arg0.getSpatial())
                    .compare(this.attribute, arg0.getAttribute())
                    .result();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(this.spatial, this.attribute);
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SpatioAttributeWritable))
                return false;
            return (this.compareTo((SpatioAttributeWritable) o) == 0) ? true : false;
        }

    }
    
    public static class AttributeResolutionWritable implements WritableComparable<AttributeResolutionWritable> {

        private int attribute;
        private int spatialResolution;
        private int temporalResolution;
        private int dataset;
        
        public AttributeResolutionWritable() {
            this.attribute = -1;
            this.spatialResolution = -1;
            this.temporalResolution = -1;
            this.dataset = -1;
        }
        
        public AttributeResolutionWritable(int attribute,
        		int spatialResolution, int temporalResolution,
        		int dataset) {
            this.attribute = attribute;
            this.spatialResolution = spatialResolution;
            this.temporalResolution = temporalResolution;
            this.dataset = dataset;
        }
        
        public int getAttribute() {
            return attribute;
        }
        
        public int getSpatialResolution() {
        	return this.spatialResolution;
        }
        
        public int getTemporalResolution() {
        	return this.temporalResolution;
        }
        
        public int getDataset() {
            return this.dataset;
        }
        
        @Override
        public String toString() {
            return String.valueOf(dataset) + "," +  String.valueOf(attribute) + "," +
                    String.valueOf(temporalResolution) + "," + String.valueOf(spatialResolution);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // attribute
            this.attribute = in.readInt();
            
            // spatial resolution
            this.spatialResolution = in.readInt();
            
            // temporal resolution
            this.temporalResolution = in.readInt();
            
            // dataset
            this.dataset = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // attribute
            out.writeInt(this.attribute);
            
            // spatial resolution
            out.writeInt(this.spatialResolution);
            
            // temporal resolution
            out.writeInt(this.temporalResolution);
            
            // dataset
            out.writeInt(this.dataset);
        }

        @Override
        public int compareTo(AttributeResolutionWritable arg0) {
            return ComparisonChain.start()
                    .compare(this.attribute, arg0.getAttribute())
                    .compare(this.spatialResolution, arg0.getSpatialResolution())
                    .compare(this.temporalResolution, arg0.getTemporalResolution())
                    .compare(this.dataset, arg0.getDataset())
                    .result();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(this.attribute,
                    this.spatialResolution, this.temporalResolution,
                    this.dataset);
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof AttributeResolutionWritable))
                return false;
            return (this.compareTo((AttributeResolutionWritable) o) == 0) ? true : false;
        }

    }
    
    public static class PairAttributeWritable implements WritableComparable<PairAttributeWritable> {

        private int firstAttribute;
        private int secondAttribute;
        private int firstDataset;
        private int secondDataset;
        
        private int spatialResolution;
        private int temporalResolution;
        private boolean isOutlier;
        
        public PairAttributeWritable() {
            this.firstAttribute = -1;
            this.secondAttribute = -1;
            this.firstDataset = -1;
            this.secondDataset = -1;
            
            this.spatialResolution = -1;
            this.temporalResolution = -1;
            this.isOutlier = false;
        }
        
        public PairAttributeWritable(int firstAttribute, int secondAttribute,
                int firstDataset, int secondDataset,
        		int spatialResolution, int temporalResolution, boolean isOutlier) {
            this.firstAttribute = firstAttribute;
            this.secondAttribute = secondAttribute;
            this.firstDataset = firstDataset;
            this.secondDataset = secondDataset;
            this.spatialResolution = spatialResolution;
            this.temporalResolution = temporalResolution;
            this.isOutlier = isOutlier;
        }
        
        public int getFirstAttribute() {
            return firstAttribute;
        }
        
        public int getSecondAttribute() {
            return secondAttribute;
        }
        
        public int getFirstDataset() {
            return firstDataset;
        }
        
        public int getSecondDataset() {
            return secondDataset;
        }
        
        public int getSpatialResolution() {
        	return this.spatialResolution;
        }
        
        public int getTemporalResolution() {
        	return this.temporalResolution;
        }
        
        public boolean getIsOutlier() {
        	return this.isOutlier;
        }
        
        @Override
        public String toString() {
            return String.valueOf(firstAttribute) + "," + String.valueOf(secondAttribute) + ","
                    + String.valueOf(firstDataset) + "," + String.valueOf(secondDataset);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // first attribute
            this.firstAttribute = in.readInt();
            
            // second attribute
            this.secondAttribute = in.readInt();
            
            // first dataset
            this.firstDataset = in.readInt();
            
            // second dataset
            this.secondDataset = in.readInt();
            
            // spatial resolution
            this.spatialResolution = in.readInt();
            
            // temporal resolution
            this.temporalResolution = in.readInt();
            
            // outlier info
            this.isOutlier = in.readBoolean();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // first attribute
            out.writeInt(this.firstAttribute);
            
            // second attribute
            out.writeInt(this.secondAttribute);
            
            // first dataset
            out.writeInt(this.firstDataset);
            
            // second dataset
            out.writeInt(this.secondDataset);
            
            // spatial resolution
            out.writeInt(this.spatialResolution);
            
            // temporal resolution
            out.writeInt(this.temporalResolution);
            
            // outlier info
            out.writeBoolean(this.isOutlier);
        }

        @Override
        public int compareTo(PairAttributeWritable arg0) {
            return ComparisonChain.start()
                    .compare(this.firstAttribute, arg0.getFirstAttribute())
                    .compare(this.secondAttribute, arg0.getSecondAttribute())
                    .compare(this.firstDataset, arg0.getFirstDataset())
                    .compare(this.secondDataset, arg0.getSecondDataset())
                    .compare(this.spatialResolution, arg0.getSpatialResolution())
                    .compare(this.temporalResolution, arg0.getTemporalResolution())
                    .compare(this.isOutlier, arg0.getIsOutlier())
                    .result();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(this.firstAttribute, this.secondAttribute,
                    this.firstDataset, this.secondDataset,
            		this.spatialResolution, this.temporalResolution, this.isOutlier);
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PairAttributeWritable))
                return false;
            return (this.compareTo((PairAttributeWritable) o) == 0) ? true : false;
        }

    }
    
    public static class TemporalFloatWritable implements WritableComparable<TemporalFloatWritable> {

        private int temporal;
        private float value;
        
        public TemporalFloatWritable() {
            this.temporal = -1;
            this.value = 0;
        }
        
        public TemporalFloatWritable(int temporal, float value) {
            this.temporal = temporal;
            this.value = value;
        }
        
        public int getTemporal() {
            return this.temporal;
        }
        
        public float getValue() {
            return this.value;
        }
        
        @Override
        public String toString() {
            return String.valueOf(temporal) + "," + String.valueOf(value);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // temporal
            this.temporal = in.readInt();
            
            // value
            this.value = in.readFloat();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // spatial
            out.writeInt(this.temporal);
            
            // attribute
            out.writeFloat(this.value);
        }

        @Override
        public int compareTo(TemporalFloatWritable arg0) {
            return ComparisonChain.start()
                    .compare(this.temporal, arg0.getTemporal())
                    .compare(this.value, arg0.getValue())
                    .result();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(this.temporal, this.value);
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TemporalFloatWritable))
                return false;
            return (this.compareTo((TemporalFloatWritable) o) == 0) ? true : false;
        }

    }
    
    public static class SpatioTemporalFloatWritable implements WritableComparable<SpatioTemporalFloatWritable> {

        private int spatial;
        private int temporal;
        private float value;
        
        public SpatioTemporalFloatWritable() {
            this.spatial = 0;
            this.temporal = -1;
            this.value = 0;
        }
        
        public SpatioTemporalFloatWritable(int spatial, int temporal, float value) {
            this.spatial = spatial;
            this.temporal = temporal;
            this.value = value;
        }
        
        public int getSpatial() {
            return this.spatial;
        }
        
        public int getTemporal() {
            return this.temporal;
        }
        
        public float getValue() {
            return this.value;
        }
        
        @Override
        public String toString() {
            return String.valueOf(spatial) + "," + String.valueOf(temporal) + "," + String.valueOf(value);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // spatial
            this.spatial = in.readInt();
            
            // temporal
            this.temporal = in.readInt();
            
            // value
            this.value = in.readFloat();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // spatial
            out.writeInt(this.spatial);
            
            // temporal
            out.writeInt(this.temporal);
            
            // attribute
            out.writeFloat(this.value);
        }

        @Override
        public int compareTo(SpatioTemporalFloatWritable arg0) {
            return ComparisonChain.start()
                    .compare(this.spatial, arg0.getSpatial())
                    .compare(this.temporal, arg0.getTemporal())
                    .compare(this.value, arg0.getValue())
                    .result();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(this.spatial, this.temporal, this.value);
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TemporalFloatWritable))
                return false;
            return (this.compareTo((SpatioTemporalFloatWritable) o) == 0) ? true : false;
        }

    }
    
    public static class TimeSeriesWritable implements WritableComparable<TimeSeriesWritable> {

        private int spatial;
        private byte dataset;
        private int attribute;
        private byte[] timeSeries;
        private int start;
        private int end;
        private int nbPosEvents;
        private int nbNegEvents;
        private int nbNonEvents;
        private float threshold;
        
        /*private HashMap<Integer,Float> mean = new HashMap<Integer,Float>();
        private HashMap<Integer,Float> stdDev = new HashMap<Integer,Float>();
        private HashMap<Integer,Float> min = new HashMap<Integer,Float>();
        private HashMap<Integer,Float> max = new HashMap<Integer,Float>();*/
        
        public TimeSeriesWritable() {
            this.spatial = 0;
            this.dataset = 0;
            this.attribute = 0;
            this.timeSeries = new byte[0];
            this.start = 0;
            this.end = 0;
            this.nbPosEvents = 0;
            this.nbNegEvents = 0;
            this.nbNonEvents = 0;
            this.threshold = 0;
        }
        
        public TimeSeriesWritable(TimeSeriesWritable object) {
            this.spatial = object.getSpatial();
            this.dataset = object.getDataset();
            this.attribute = object.getAttribute();
            this.timeSeries = Arrays.copyOf(object.getTimeSeries(), object.getTimeSeries().length);
            this.start = object.getStart();
            this.end = object.getEnd();
            this.nbPosEvents = object.getNbPosEvents();
            this.nbNegEvents = object.getNbNegEvents();
            this.nbNonEvents = object.getNbNonEvents();
            this.threshold = object.getThreshold();
            
            /*this.mean = object.getMean();
            this.stdDev = object.getStdDev();
            this.min = object.getMin();
            this.max = object.getMax();*/
            
        }
        
        public TimeSeriesWritable(
                int spatial,
                byte dataset,
                int attribute,
                byte[] timeSeries,
                int start,
                int end,
                int nbPosEvents,
                int nbNegEvents,
                int nbNonEvents,
                float threshold) {
                //HashMap<Integer,Float> mean,
                //HashMap<Integer,Float> stdDev,
                //HashMap<Integer,Float> min,
                //HashMap<Integer,Float> max) {
            this.spatial = spatial;
            this.dataset = dataset;
            this.attribute = attribute;
            this.timeSeries = timeSeries;
            this.start = start;
            this.end = end;
            this.nbPosEvents = nbPosEvents;
            this.nbNegEvents = nbNegEvents;
            this.nbNonEvents = nbNonEvents;
            this.threshold = threshold;
            
            /*this.mean = mean;
            this.stdDev = stdDev;
            this.max = max;
            this.min = min;*/
        }
        
        public int getSpatial() {
            return this.spatial;
        }
        
        public byte getDataset() {
            return this.dataset;
        }
        
        public int getAttribute() {
            return this.attribute;
        }
        
        public byte[] getTimeSeries() {
            return this.timeSeries;
        }
        
        public int getStart() {
            return this.start;
        }
        
        public int getEnd() {
            return this.end;
        }
        
        public int getNbPosEvents() {
            return this.nbPosEvents;
        }
        
        public int getNbNegEvents() {
            return this.nbNegEvents;
        }
        
        public int getNbNonEvents() {
            return this.nbNonEvents;
        }
        
        public float getThreshold() {
            return this.threshold;
        }
        
        /*public HashMap<Integer,Float> getMean() {
        	return this.mean;
        }
        
        public HashMap<Integer,Float> getStdDev() {
        	return this.stdDev;
        }
        
        public HashMap<Integer,Float> getMin() {
        	return this.min;
        }
        
        public HashMap<Integer,Float> getMax() {
        	return this.max;
        }*/
        
        @Override
        public String toString() {
            String result = "";
            for (int i = 0; i < timeSeries.length; i++)
                result += timeSeries[i] + ",";
            return result.substring(0, result.length()-1);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            spatial = in.readInt();
            dataset = in.readByte();
            attribute = in.readInt();
            timeSeries = new byte[in.readInt()];
            for (int i = 0; i < timeSeries.length; i++)
                timeSeries[i] = in.readByte();
            start = in.readInt();
            end = in.readInt();
            nbPosEvents = in.readInt();
            nbNegEvents = in.readInt();
            nbNonEvents = in.readInt();
            threshold = in.readFloat();
            
            /*int size = in.readInt();
            mean = new HashMap<Integer,Float>(size);
            for (int i = 0; i < size; i++) {
                int k = in.readInt();
                mean.put(k, in.readFloat());
            }
            stdDev = new HashMap<Integer,Float>(size);
            for (int i = 0; i < size; i++) {
                int k = in.readInt();
                stdDev.put(k, in.readFloat());
            }
            min = new HashMap<Integer,Float>(size);
            for (int i = 0; i < size; i++) {
                int k = in.readInt();
                min.put(k, in.readFloat());
            }
            max = new HashMap<Integer,Float>(size);
            for (int i = 0; i < size; i++) {
                int k = in.readInt();
                max.put(k, in.readFloat());
            }*/
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(spatial);
            out.writeByte(dataset);
            out.writeInt(attribute);
            out.writeInt(timeSeries.length);
            for (int i = 0; i < timeSeries.length; i++)
                out.writeByte(timeSeries[i]);
            out.writeInt(start);
            out.writeInt(end);
            out.writeInt(nbPosEvents);
            out.writeInt(nbNegEvents);
            out.writeInt(nbNonEvents);
            out.writeFloat(threshold);
            
            /*out.writeInt(mean.size());
            for (Integer k: mean.keySet()) {
            	out.writeInt(k);
            	out.writeFloat(mean.get(k));
            }
            for (Integer k: stdDev.keySet()) {
            	out.writeInt(k);
            	out.writeFloat(stdDev.get(k));
            }
            
            if(mean.size() != min.size()) {
            	throw new IOException("Min Size different than Mean size");
            }
            
            for (Integer k: min.keySet()) {
            	out.writeInt(k);
            	out.writeFloat(min.get(k));
            }
            
            if(mean.size() != max.size()) {
            	throw new IOException("Max Size different than Mean size");
            }
            
            for (Integer k: max.keySet()) {
            	out.writeInt(k);
            	out.writeFloat(max.get(k));
            }*/
        }

        @Override
        public int compareTo(TimeSeriesWritable arg0) {
            return 0;
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(
                    this.spatial,
                    this.dataset,
                    this.attribute,
                    this.timeSeries,
                    this.start,
                    this.end,
                    this.nbPosEvents,
                    this.nbNegEvents,
                    this.nbNonEvents,
                    this.threshold);
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TimeSeriesWritable))
                return false;
            return (this.compareTo((TimeSeriesWritable) o) == 0) ? true : false;
        }

    }
    
    public static class TopologyTimeSeriesWritable implements WritableComparable<TopologyTimeSeriesWritable> {

        private int spatial;
        private int dataset;
        private byte[] timeSeries;
        private int start;
        private int end;
        private boolean isOutlier;
        
        public TopologyTimeSeriesWritable() {
            this.spatial = 0;
            this.dataset = 0;
            this.timeSeries = new byte[0];
            this.start = 0;
            this.end = 0;
            this.isOutlier = false;
        }
        
        public TopologyTimeSeriesWritable(TopologyTimeSeriesWritable object) {
            this.spatial = object.getSpatial();
            this.dataset = object.getDataset();
            this.timeSeries = Arrays.copyOf(object.getTimeSeries(), object.getTimeSeries().length);
            this.start = object.getStart();
            this.end = object.getEnd();
            this.isOutlier = object.getIsOutlier();
        }
        
        public TopologyTimeSeriesWritable(
                int spatial,
                int dataset,
                byte[] timeSeries,
                int start,
                int end,
                boolean isOutlier) {
            this.spatial = spatial;
            this.dataset = dataset;
            this.timeSeries = timeSeries;
            this.start = start;
            this.end = end;
            this.isOutlier = isOutlier;
        }
        
        public int getSpatial() {
            return this.spatial;
        }
        
        public int getDataset() {
            return this.dataset;
        }
        
        public byte[] getTimeSeries() {
            return this.timeSeries;
        }
        
        public int getStart() {
            return this.start;
        }
        
        public int getEnd() {
            return this.end;
        }
        
        public boolean getIsOutlier() {
            return this.isOutlier;
        }

        public String toString(int tempRes) {
            String result = spatial + "," + isOutlier + ",";
            
            int timeSteps = getTimeSteps(tempRes, start, end);
            DateTime startTime = new DateTime(((long)start)*1000, DateTimeZone.UTC);
            if (timeSteps != timeSeries.length) {
                System.out.println("Something is wrong... Wrong time steps length");
                System.exit(-1);
            }
            for (int i = 0; i < timeSteps; i++) {
                long time = addTimeSteps(tempRes, i, startTime);
                result += String.valueOf(time) + "," + timeSeries[i] + ",";
            }
            return result.substring(0, result.length()-1);
        }
        
        @Override
        public String toString() {
            String result = dataset + "," + spatial + "," + isOutlier + ",";
            result += String.valueOf(start) + "," + String.valueOf(end) + ","; 
            for (int i = 0; i < timeSeries.length; i++)
                result += timeSeries[i] + ",";
            return result.substring(0, result.length()-1);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            spatial = in.readInt();
            dataset = in.readInt();
            timeSeries = new byte[in.readInt()];
            for (int i = 0; i < timeSeries.length; i++)
                timeSeries[i] = in.readByte();
            start = in.readInt();
            end = in.readInt();
            isOutlier = in.readBoolean();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(spatial);
            out.writeInt(dataset);
            out.writeInt(timeSeries.length);
            for (int i = 0; i < timeSeries.length; i++)
                out.writeByte(timeSeries[i]);
            out.writeInt(start);
            out.writeInt(end);
            out.writeBoolean(isOutlier);
        }

        @Override
        public int compareTo(TopologyTimeSeriesWritable arg0) {
        	return ComparisonChain.start()
                    .compare(this.spatial, arg0.getSpatial())
                    .compare(this.dataset, arg0.getDataset())
                    .compare(this.start, arg0.getStart())
                    .compare(this.end, arg0.getEnd())
                    .compare(this.isOutlier, arg0.getIsOutlier())
                    .result();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(
                    this.spatial,
                    this.dataset,
                    this.timeSeries,
                    this.start,
                    this.end,
                    this.isOutlier);
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TopologyTimeSeriesWritable))
                return false;
            return (this.compareTo((TopologyTimeSeriesWritable) o) == 0) ? true : false;
        }

    }
    
    public static class FloatTimeSeriesWritable implements WritableComparable<FloatTimeSeriesWritable> {

        private int spatial;
        private int dataset;
        private int attribute;
        private HashMap<Integer,Float> timeSeries;
        private int start;
        private int end;
        
        public FloatTimeSeriesWritable() {
            this.spatial = 0;
            this.dataset = 0;
            this.attribute = 0;
            this.timeSeries = new HashMap<Integer,Float>();
            this.start = 0;
            this.end = 0;
        }
        
        public FloatTimeSeriesWritable(FloatTimeSeriesWritable object) {
            this.spatial = object.getSpatial();
            this.dataset = object.getDataset();
            this.attribute = object.getAttribute();
            this.timeSeries = new HashMap<Integer,Float>(object.getTimeSeries());
            this.start = object.getStart();
            this.end = object.getEnd();
        }
        
        public FloatTimeSeriesWritable(
                int spatial,
                int dataset,
                int attribute,
                HashMap<Integer,Float> timeSeries,
                int start,
                int end) {
            this.spatial = spatial;
            this.dataset = dataset;
            this.attribute = attribute;
            this.timeSeries = timeSeries;
            this.start = start;
            this.end = end;
        }
        
        public int getSpatial() {
            return this.spatial;
        }
        
        public int getDataset() {
            return this.dataset;
        }
        
        public int getAttribute() {
            return this.attribute;
        }
        
        public HashMap<Integer,Float> getTimeSeries() {
            return this.timeSeries;
        }
        
        public int getStart() {
            return this.start;
        }
        
        public int getEnd() {
            return this.end;
        }
        
        public String toString(int tempRes) {
            return null;
        }
        
        @Override
        public String toString() {
            return null;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            spatial = in.readInt();
            dataset = in.readInt();
            attribute = in.readInt();
            int size = in.readInt();
            timeSeries = new HashMap<Integer,Float>(size);
            for (int i = 0; i < size; i++) {
                int temp = in.readInt();
                timeSeries.put(temp, in.readFloat());
            }
            start = in.readInt();
            end = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(spatial);
            out.writeInt(dataset);
            out.writeInt(attribute);
            out.writeInt(timeSeries.size());
            for (int temp : timeSeries.keySet()) {
                out.writeInt(temp);
                out.writeFloat(timeSeries.get(temp));
            }
            out.writeInt(start);
            out.writeInt(end);
        }

        @Override
        public int compareTo(FloatTimeSeriesWritable arg0) {
            return ComparisonChain.start()
                    .compare(this.spatial, arg0.getSpatial())
                    .compare(this.dataset, arg0.getDataset())
                    .compare(this.attribute, arg0.getAttribute())
                    .compare(this.start, arg0.getStart())
                    .compare(this.end, arg0.getEnd())
                    .result();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(
                    this.spatial,
                    this.dataset,
                    this.attribute,
                    this.timeSeries,
                    this.start,
                    this.end);
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof FloatTimeSeriesWritable))
                return false;
            return (this.compareTo((FloatTimeSeriesWritable) o) == 0) ? true : false;
        }

    }
    
    public static class FloatArrayWritable implements WritableComparable<FloatArrayWritable> {
        
        private float[] values;
        
        public FloatArrayWritable(ArrayList<Float> values) {
            set(values);
        }
        
        public FloatArrayWritable(float[] values) {
            set(values);
        }
        
        public FloatArrayWritable() {
            values = new float[0];
        }
        
        public void set(ArrayList<Float> values) {
            this.values =new float[values.size()]; 
            for(int i = 0;i < this.values.length;i ++) {
            	this.values[i] = values.get(i);
            }
        }
        
        public void set(float[] values) {
            this.values = Arrays.copyOf(values, values.length);
        }
        
        public float[] get() {
            return this.values;
        }
        
        @Override
        public String toString() {
            String result = "";
            for (int i = 0; i < this.values.length; i++)
                result += this.values[i] + ",";
            return result.substring(0, result.length()-1);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.values.length);
            for (int i = 0; i < values.length; i++)
                out.writeFloat(this.values[i]);
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            this.values = new float[in.readInt()];
            for (int i = 0; i < values.length; i++)
                this.values[i] = in.readFloat();
        }

        @Override
        public int compareTo(FloatArrayWritable arg0) {
        	float[] thatVal = arg0.get();
            
            ComparisonChain ch = ComparisonChain.start();
            // assuming they have the same size
            for (int i = 0; i < this.values.length; i++)
                ch = ch.compare(this.values[i], thatVal[i]);
            return ch.result();
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof FloatArrayWritable))
                return false;
            return (this.compareTo((FloatArrayWritable) o) == 0) ? true : false;
        }
        
        @Override
        public int hashCode() {
            return values.hashCode();
        }
    }
    
    public static class SpatioTemporalValueWritable implements WritableComparable<SpatioTemporalValueWritable> {

        private int spatial;
        private int temporal;
        private int dataset;
        private int attribute;
        private float value;
        
        public SpatioTemporalValueWritable() {
            this.spatial = 0;
            this.temporal = 0;
            this.dataset = 0;
            this.attribute = 0;
            this.value = 0;
        }
        
        public SpatioTemporalValueWritable(SpatioTemporalValueWritable object) {
            this.spatial = object.getSpatial();
            this.temporal = object.getTemporal();
            this.dataset = object.getDataset();
            this.attribute = object.getAttribute();
            this.value = object.getValue();
        }
        
        public SpatioTemporalValueWritable(
                int spatial,
                int temporal,
                int dataset,
                int attribute,
                float value) {
            this.spatial = spatial;
            this.temporal = temporal;
            this.dataset = dataset;
            this.attribute = attribute;
            this.value = value;
        }
        
        public int getSpatial() {
            return this.spatial;
        }
        
        public int getTemporal() {
            return this.temporal;
        }
        
        public int getDataset() {
            return this.dataset;
        }
        
        public int getAttribute() {
            return this.attribute;
        }
        
        public float getValue() {
            return this.value;
        }
        
        public String toString(int tempRes) {
            return null;
        }
        
        @Override
        public String toString() {
            return null;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            spatial = in.readInt();
            temporal = in.readInt();
            dataset = in.readInt();
            attribute = in.readInt();
            value = in.readFloat();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(spatial);
            out.writeInt(temporal);
            out.writeInt(dataset);
            out.writeInt(attribute);
            out.writeFloat(value);
        }

        @Override
        public int compareTo(SpatioTemporalValueWritable arg0) {
            return ComparisonChain.start()
                    .compare(this.spatial, arg0.getSpatial())
                    .compare(this.temporal, arg0.getTemporal())
                    .compare(this.dataset, arg0.getDataset())
                    .compare(this.attribute, arg0.getAttribute())
                    .compare(this.value, arg0.getValue())
                    .result();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(
                    this.spatial,
                    this.temporal,
                    this.dataset,
                    this.attribute,
                    this.value);
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SpatioTemporalValueWritable))
                return false;
            return (this.compareTo((SpatioTemporalValueWritable) o) == 0) ? true : false;
        }

    }
    
    public static class AggregationArrayWritable implements WritableComparable<AggregationArrayWritable> {

        private Aggregation[] aggregations;
        
        public AggregationArrayWritable() {
            aggregations = new Aggregation[0];
        }
        
        public AggregationArrayWritable(ArrayList<Aggregation> agg) {
            this.aggregations = agg.toArray(new Aggregation[0]);
        }
        
        public AggregationArrayWritable(Aggregation[] agg) {
            this.aggregations = Arrays.copyOf(agg, agg.length);
        }
        
        public Aggregation[] get() {
            return aggregations;
        }
        
        @Override
        public String toString() {
            String result = "";
            for (int i = 0; i < this.aggregations.length; i++)
                result += this.aggregations[i].getResult() + ",";
            return result.substring(0, result.length()-1);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            aggregations = new Aggregation[in.readInt()];
            for (int i = 0; i < aggregations.length; i++) {
                aggregations[i] = getAggregation(in.readInt());
                aggregations[i].readFields(in);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(aggregations.length);
            for (int i = 0; i < aggregations.length; i++) {
                out.writeInt(aggregations[i].getId().ordinal());
                aggregations[i].write(out);
            }
        }

        @Override
        public int compareTo(AggregationArrayWritable arg0) {
            return 0;
        }
        
        @Override
        public boolean equals(Object o) {
            if (!(o instanceof AggregationArrayWritable))
                return false;
            //return (this.compareTo((AggregationArrayWritable) o) == 0) ? true : false;
            return true;
        }
    }
    
    /**
     * Time Series
     */
    
    public static final byte positiveEvent = 2;
    public static final byte negativeEvent = 4;
    public static final byte nonEvent = 1;
    
    public static final byte nonEventsMatch = (byte) (nonEvent | nonEvent);
    public static final byte posEventsMatch = (byte) (positiveEvent | positiveEvent);
    public static final byte negEventsMatch = (byte) (negativeEvent | negativeEvent);
    public static final byte nonEventPosEventMatch = (byte) (nonEvent | positiveEvent);
    public static final byte nonEventNegEventMatch = (byte) (nonEvent | negativeEvent);
    public static final byte negEventPosEventMatch = (byte) (negativeEvent | positiveEvent);
    
    public static float[] eventThresholds = {1.0f, 1.5f, 1.75f, 2.0f, 2.5f};
    
    public static class TimeSeriesStats {
        
        private int nMatchEvents = 0;
        private int nMatchPosEvents = 0;
        private int nMatchNegEvents = 0;
        private int nPosFirstPosSecond = 0;
        private int nNegFirstNegSecond = 0;
        private int nPosFirstNegSecond = 0;
        private int nNegFirstPosSecond = 0;
        private int nPosFirstNonSecond = 0;
        private int nNegFirstNonSecond = 0;
        private int nNonFirstPosSecond = 0;
        private int nNonFirstNegSecond = 0;
        
        private float relationshipScore = 0;
        private float relationshipStrength = 0;
        private float precision = 0;
        private float recall = 0;
        
        private boolean intersect = false;
        
        private DateTime start = null;
        private DateTime end = null;
        
        /*private HashSet<String> matchPosEvents = new HashSet<String>();
        private HashSet<String> matchNegEvents = new HashSet<String>();*/
        
        public TimeSeriesStats() {}
        
        public void setParameters(
                int nMatchEvents,
                int nMatchPosEvents,
                int nMatchNegEvents,
                int nPosFirstPosSecond,
                int nNegFirstNegSecond,
                int nPosFirstNegSecond,
                int nNegFirstPosSecond,
                int nPosFirstNonSecond,
                int nNegFirstNonSecond,
                int nNonFirstPosSecond,
                int nNonFirstNegSecond) {
            this.setMatchEvents(nMatchEvents);
            this.setMatchPosEvents(nMatchPosEvents);
            this.setMatchNegEvents(nMatchNegEvents);
            this.setPosFirstPosSecond(nPosFirstPosSecond);
            this.setNegFirstNegSecond(nNegFirstNegSecond);
            this.setPosFirstNegSecond(nPosFirstNegSecond);
            this.setNegFirstPosSecond(nNegFirstPosSecond);
            this.setPosFirstNonSecond(nPosFirstNonSecond);
            this.setNegFirstNonSecond(nNegFirstNonSecond);
            this.setNonFirstPosSecond(nNonFirstPosSecond);
            this.setNonFirstNegSecond(nNonFirstNegSecond);
        }
        
        /*public void addMatchPosEvents(String dt) {
            matchPosEvents.add(dt);
        }
        
        public void addMatchNegEvents(String dt) {
            matchNegEvents.add(dt);
        }
        
        public HashSet<String> getPosEvents() {
            return this.matchPosEvents;
        }
        
        public HashSet<String> getNegEvents() {
            return this.matchNegEvents;
        }*/
        
        public void setStart(DateTime start) {
            this.start = start;
        }
        
        public void setEnd(DateTime end) {
            this.end = end;
        }
        
        public DateTime getStart() {
            return start;
        }
        
        public DateTime getEnd() {
            return end;
        }

        public int getMatchEvents() {
            return nMatchEvents;
        }

        public void setMatchEvents(int nMatchEvents) {
            this.nMatchEvents = nMatchEvents;
        }

        public int getMatchPosEvents() {
            return nMatchPosEvents;
        }

        public void setMatchPosEvents(int nMatchPosEvents) {
            this.nMatchPosEvents = nMatchPosEvents;
        }

        public int getMatchNegEvents() {
            return nMatchNegEvents;
        }

        public void setMatchNegEvents(int nMatchNegEvents) {
            this.nMatchNegEvents = nMatchNegEvents;
        }
        
        public int getPosFirstPosSecond() {
            return nPosFirstPosSecond;
        }

        public void setPosFirstPosSecond(int nPosFirstPosSecond) {
            this.nPosFirstPosSecond = nPosFirstPosSecond;
        }

        public int getNegFirstNegSecond() {
            return nNegFirstNegSecond;
        }

        public void setNegFirstNegSecond(int nNegFirstNegSecond) {
            this.nNegFirstNegSecond = nNegFirstNegSecond;
        }

        public int getPosFirstNegSecond() {
            return nPosFirstNegSecond;
        }

        public void setPosFirstNegSecond(int nPosFirstNegSecond) {
            this.nPosFirstNegSecond = nPosFirstNegSecond;
        }

        public int getNegFirstPosSecond() {
            return nNegFirstPosSecond;
        }

        public void setNegFirstPosSecond(int nNegFirstPosSecond) {
            this.nNegFirstPosSecond = nNegFirstPosSecond;
        }

        public int getPosFirstNonSecond() {
            return nPosFirstNonSecond;
        }

        public void setPosFirstNonSecond(int nPosFirstNonSecond) {
            this.nPosFirstNonSecond = nPosFirstNonSecond;
        }

        public int getNegFirstNonSecond() {
            return nNegFirstNonSecond;
        }

        public void setNegFirstNonSecond(int nNegFirstNonSecond) {
            this.nNegFirstNonSecond = nNegFirstNonSecond;
        }

        public int getNonFirstPosSecond() {
            return nNonFirstPosSecond;
        }

        public void setNonFirstPosSecond(int nNonFirstPosSecond) {
            this.nNonFirstPosSecond = nNonFirstPosSecond;
        }

        public int getNonFirstNegSecond() {
            return nNonFirstNegSecond;
        }

        public void setNonFirstNegSecond(int nNonFirstNegSecond) {
            this.nNonFirstNegSecond = nNonFirstNegSecond;
        }
        
        public boolean isIntersect() {
            return intersect;
        }

        public void setIntersect(boolean intersect) {
            this.intersect = intersect;
        }

        public float getRelationshipScore() {
            return relationshipScore;
        }
        
        public float getRelationshipStrength() {
            return relationshipStrength;
        }
        
        public float getRecall() {
            return recall;
        }
        
        public float getPrecision() {
            return precision;
        }
        
        public void add(TimeSeriesStats stats) {
            this.nMatchEvents += stats.getMatchEvents();
            this.nMatchPosEvents += stats.getMatchPosEvents();
            this.nMatchNegEvents += stats.getMatchNegEvents();
            this.nPosFirstPosSecond += stats.getPosFirstPosSecond();
            this.nNegFirstNegSecond += stats.getNegFirstNegSecond();
            this.nPosFirstNegSecond += stats.getPosFirstNegSecond();
            this.nNegFirstPosSecond += stats.getNegFirstPosSecond();
            this.nNegFirstNonSecond += stats.getNegFirstNonSecond();
            this.nPosFirstNonSecond += stats.getPosFirstNonSecond();
            this.nNonFirstPosSecond += stats.getNonFirstPosSecond();
            this.nNonFirstNegSecond += stats.getNonFirstNegSecond();
            this.intersect = this.intersect | stats.isIntersect();
            
            /*for (String dt : stats.getPosEvents()) {
                this.addMatchPosEvents(dt);
            }
            
            for (String dt : stats.getNegEvents()) {
                this.addMatchNegEvents(dt);
            }*/
            
            DateTime statsStart = stats.getStart();
            if ((statsStart != null) && (this.start != null))
                this.start = (this.start.isBefore(statsStart)) ? this.start : statsStart;
            DateTime statsEnd = stats.getEnd();
            if ((statsEnd != null) && (this.end != null))
                this.end = (this.end.isAfter(statsEnd)) ? this.end : statsEnd;
        }
        
        public void computeScores() {
            
            /*
             * Score
             */
            
            relationshipScore = (nMatchEvents > 0) ? (nMatchPosEvents - nMatchNegEvents)/(float)(nMatchEvents) : 0;
            
            /*
             * Strength
             */

            int tp = nMatchEvents;
            int fp = nNonFirstPosSecond + nNonFirstNegSecond;
            int fn = nPosFirstNonSecond + nNegFirstNonSecond;
            
            // relevance of the correlation for first attribute
            recall = ((tp + fn) > 0) ? tp / (float)(tp + fn) : 0;
            
            // relevance of the correlation for second attribute
            precision = ((tp +fp) > 0) ? tp / (float)(tp + fp) : 0;
            
            relationshipStrength = ((precision + recall) > 0) ? 2* (precision*recall) / (precision + recall) : 0;
        }
        
    }
    
    /**
     * HDFS/S3 File Handling
     */
    
    public static final String preProcessingDir = "pre-processing";
    public static final String aggregatesDir = "aggregates";
    public static final String aggregatesTextDir = "aggregates-text";
    public static final String indexDir = "index";
    public static final String indexTextDir = "index-text";
    public static final String mergeTreeDir = "mergetree";
    public static final String relationshipsDir = "relationships";
    public static final String relationshipsIdsDir = "relationships-ids";
    public static final String dataDir = "data";
    public static final String thresholdDir = dataDir + "/" + "thresholds";
    public static final String datasetsIndexDir = dataDir + "/" + "datasets";
    public static final String dataAttributesDir = "attributes";
    public static final String correlationTechniquesDir = "correlations";
    
    public static boolean fileExists(String fileName, Configuration conf, boolean s3) throws IOException {
    	boolean result = false;
    	
    	if (s3) {
        	Path path = new Path(fileName);
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            
            if (fs.exists(path))
                result = true;
            
            fs.close();
            return result;
    	} else {
    	    FileSystem hdfs = FileSystem.get(new Configuration());
            Path hdfsFile = new Path(hdfs.getHomeDirectory() + "/" + fileName);
            
            if (hdfs.exists(hdfsFile))
                return true;
            return false;
    	}
    }
    
    public static void removeFile(String fileName, Configuration conf, boolean s3) throws IOException {
        if (s3) {
        	Path path = new Path(fileName);
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            
            if (fs.exists(path))
                fs.delete(path, true);
            
            fs.close();
        } else {
            FileSystem hdfs = FileSystem.get(new Configuration());
            Path hdfsFile = new Path(hdfs.getHomeDirectory() + "/" + fileName);

            if (hdfs.exists(hdfsFile))
                hdfs.delete(hdfsFile, true);
        }
    }
    
    public static void renameFile(String from, String to, Configuration conf, boolean s3) throws IOException {
        if (s3) {
            Path pathFrom = new Path(from);
            Path pathTo = new Path(to);
            FileSystem fs = FileSystem.get(pathFrom.toUri(), conf);
            
            fs.rename(pathFrom, pathTo);
            
            fs.close();
        } else {
            FileSystem hdfs = FileSystem.get(new Configuration());
            Path pathFrom = new Path(hdfs.getHomeDirectory() + "/" + from);
            Path pathTo = new Path(hdfs.getHomeDirectory() + "/" + to);

            hdfs.rename(pathFrom, pathTo);
        }
    }
    
    public static void createDir(String dir, Configuration conf, boolean s3) throws IOException {
        if (s3) {
        	Path path = new Path(dir);
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            
            if (!fs.exists(path))
                fs.mkdirs(path);
            
            fs.close();
        } else {
            FileSystem hdfs = FileSystem.get(new Configuration());
            Path hdfsFile = new Path(hdfs.getHomeDirectory() + "/" + dir);

            if (!hdfs.exists(hdfsFile))
                hdfs.mkdirs(hdfsFile);
        }
    }
    
    public static FSDataOutputStream createFile(String file, Configuration conf, boolean s3) throws IOException { 
        if (s3) {
            Path path = new Path(file);
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            
            FSDataOutputStream outputStream = fs.create(path);
            fs.close();
            
            return outputStream;
        } else {
            FileSystem hdfs = FileSystem.get(new Configuration());
            Path hdfsFile = new Path(hdfs.getHomeDirectory() + "/" + file);

            return hdfs.create(hdfsFile);
        }
    }
    
    public static FSDataInputStream openFile(String file, Configuration conf, boolean s3) throws IOException { 
        if (s3) {
            Path path = new Path(file);
            FileSystem fs = FileSystem.get(path.toUri(), conf);
            
            if (!fs.exists(path)) {
                System.out.println("Something went wrong... File does not exist: " + path.toString());
                System.exit(1);
            }
            
            FSDataInputStream inputStream = fs.open(path);
            fs.close();
            
            return inputStream;
        } else {
            FileSystem hdfs = FileSystem.get(new Configuration());
            Path hdfsFile = new Path(hdfs.getHomeDirectory() + "/" + file);
            
            if (!hdfs.exists(hdfsFile)) {
                System.out.println("Something went wrong... File does not exist: " + hdfsFile.toString());
                System.exit(1);
            }

            return hdfs.open(hdfsFile);
        }
    }
    
    public static void removeReducerFiles(final String fileName) throws IOException {
    	PathFilter filter = new PathFilter() {

            @Override
            public boolean accept(Path arg0) {
                if (arg0.getName().contains(fileName))
                    return true;
                return false;
            }
        };
        
        FileSystem fs = FileSystem.get(new Configuration());
        Path path = new Path (fs.getHomeDirectory() + "/relationships");
        
        FileStatus[] status = fs.listStatus(path, filter);
        
        for (FileStatus fileStatus: status) {
        	fs.delete(new Path(fs.getHomeDirectory() + "/relationships/" + fileStatus.getPath().getName()), true);
        }
    }
    
    public static String searchPreProcessing(final String name, Configuration conf, boolean s3) throws IOException {
        
        PathFilter filter = new PathFilter() {

            @Override
            public boolean accept(Path arg0) {
                if (arg0.getName().contains(name + "-"))
                    return true;
                return false;
            }
        };
        
        Path path = null;
        FileSystem fs = null;
        
        if (s3) {
            path = new Path(conf.get("bucket") + preProcessingDir);
            fs = FileSystem.get(path.toUri(), conf);
        } else {
            fs = FileSystem.get(new Configuration());
            path = new Path (fs.getHomeDirectory() + "/" + preProcessingDir);
        }
        
        FileStatus[] status = fs.listStatus(path, filter);
        
        if (s3)
        	fs.close();
        
        String preProcessingFile = null;
        boolean aggregatesFound = false;
        String fileName = "";
        for (FileStatus fileStatus: status) {
            fileName = fileStatus.getPath().getName();
            if (!fileName.endsWith(".aggregates"))
                preProcessingFile = fileName;
            else if (fileName.endsWith(".aggregates"))
                aggregatesFound = true;
        }
        
        if (!aggregatesFound)
            return null;
        
        return preProcessingFile;
    }
    
    public static ArrayList<String> searchAllPreProcessing(final String name,
            Configuration conf, boolean s3) throws IOException {
        
        PathFilter filter = new PathFilter() {

            @Override
            public boolean accept(Path arg0) {
                if (arg0.getName().contains(name + "-"))
                    return true;
                return false;
            }
        };
        
        Path path = null;
        FileSystem fs = null;
        
        if (s3) {
            path = new Path(conf.get("bucket") + preProcessingDir);
            fs = FileSystem.get(path.toUri(), conf);
        } else {
            fs = FileSystem.get(new Configuration());
            path = new Path (fs.getHomeDirectory() + "/" + preProcessingDir);
        }
        
        FileStatus[] status = fs.listStatus(path, filter);
        
        if (s3)
            fs.close();
        
        ArrayList<String> preProcessingFiles = new ArrayList<String>();
        boolean aggregatesFound = false;
        String fileName = "";
        for (FileStatus fileStatus: status) {
            fileName = fileStatus.getPath().getName();
            if (!fileName.endsWith(".aggregates"))
                preProcessingFiles.add(fileName);
            else if (fileName.endsWith(".aggregates"))
                aggregatesFound = true;
        }
        
        if (!aggregatesFound)
            return null;
        
        return preProcessingFiles;
    }
    
    public static String searchAggregatesHeader(final String name, Configuration conf, boolean s3) throws IOException {
        
        PathFilter filter = new PathFilter() {

            @Override
            public boolean accept(Path arg0) {
                if (arg0.getName().contains(name + "-"))
                    return true;
                return false;
            }
        };
        
        Path path = null;
        FileSystem fs = null;
        
        if (s3) {
            path = new Path(conf.get("bucket") + preProcessingDir);
            fs = FileSystem.get(path.toUri(), conf);
        } else {
            fs = FileSystem.get(new Configuration());
            path = new Path (fs.getHomeDirectory() + "/" + preProcessingDir);
        }
        
        FileStatus[] status = fs.listStatus(path, filter);
        
        if (s3)
        	fs.close();
        
        String fileName = "";
        for (FileStatus fileStatus: status) {
            fileName = fileStatus.getPath().getName();
            if (fileName.endsWith(".aggregates"))
                return fileName;
        }
        
        return null;
    }
    
    public static String[] searchAggregates(final String name, Configuration conf, boolean s3) throws IOException {
        
    	PathFilter filter = new PathFilter() {

            @Override
            public boolean accept(Path arg0) {
                if (arg0.getName().contains("_SUCCESS"))
                    return false;
                return true;
            }
        };
    	
        Path path = null;
        FileSystem fs = null;
        
        if (s3) {
            path = new Path(conf.get("bucket") + aggregatesDir + "/" + name);
            fs = FileSystem.get(path.toUri(), conf);
        } else {
            fs = FileSystem.get(new Configuration());
            path = new Path (fs.getHomeDirectory() + "/" + aggregatesDir + "/" + name);
        }
        
        FileStatus[] status;
        
        try {
        	status = fs.listStatus(path, filter);
        } catch (FileNotFoundException e) {
        	return new String[0];
        }
        
        if (s3)
        	fs.close();
        
        String[] names = new String[status.length];
        String fileName = "";
        for (int i = 0; i < status.length; i++) {
            fileName = status[i].getPath().getName();
            names[i] = fileName;
        }
        
        return names;
    }
    
    public static String[] searchIndex(final String name, Configuration conf, boolean s3) throws IOException {
        
    	PathFilter filter = new PathFilter() {

            @Override
            public boolean accept(Path arg0) {
                if (arg0.getName().contains("_SUCCESS"))
                    return false;
                return true;
            }
        };
        
        Path path = null;
        FileSystem fs = null;
        
        if (s3) {
            path = new Path(conf.get("bucket") + indexDir + "/" + name);
            fs = FileSystem.get(path.toUri(), conf);
        } else {
            fs = FileSystem.get(new Configuration());
            path = new Path (fs.getHomeDirectory() + "/" + indexDir + "/" + name);
        }
        
        FileStatus[] status;
        
        try {
            status = fs.listStatus(path, filter);
        } catch (FileNotFoundException e) {
            return new String[0];
        }
        
        if (s3)
        	fs.close();
        
        String[] names = new String[status.length];
        String fileName = "";
        for (int i = 0; i < status.length; i++) {
            fileName = status[i].getPath().getName();
            names[i] = fileName;
        }
        
        return names;
    }
    
    public static String[] searchDataAttributes(final String name, Configuration conf, boolean s3) throws IOException {
        
        PathFilter filter = new PathFilter() {

            @Override
            public boolean accept(Path arg0) {
                if (arg0.getName().contains("_SUCCESS"))
                    return false;
                return true;
            }
        };
        
        Path path = null;
        FileSystem fs = null;
        
        if (s3) {
            path = new Path(conf.get("bucket") + dataAttributesDir + "/" + name);
            fs = FileSystem.get(path.toUri(), conf);
        } else {
            fs = FileSystem.get(new Configuration());
            path = new Path (fs.getHomeDirectory() + "/" + dataAttributesDir + "/" + name);
        }
        
        FileStatus[] status;
        
        try {
            status = fs.listStatus(path, filter);
        } catch (FileNotFoundException e) {
            return new String[0];
        }
        
        if (s3)
            fs.close();
        
        String[] names = new String[status.length];
        String fileName = "";
        for (int i = 0; i < status.length; i++) {
            fileName = status[i].getPath().getName();
            names[i] = fileName;
        }
        
        return names;
    }
    
    public static <K, V> void merge(Path fromDirectory,
            Path toFile, Class<K> keyClass, Class<V> valueClass) throws IOException {
        
        Configuration conf = new Configuration();
        
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Writer writer = SequenceFile.createWriter(
                conf,
                SequenceFile.Writer.file(toFile),
                SequenceFile.Writer.keyClass(keyClass),
                SequenceFile.Writer.valueClass(valueClass)
                );

        for (FileStatus status : fs.listStatus(fromDirectory)) {
            if (status.isDirectory()) {
                System.out.println("Skip directory '" + status.getPath().getName() + "'");
                continue;
            }

            Path file = status.getPath();

            if (file.getName().startsWith("_")) {
                System.out.println("Skip \"_\"-file '" + file.getName() + "'"); //There are files such "_SUCCESS"-named in jobs' ouput folders 
                continue;
            }

            //System.out.println("Merging '" + file.getName() + "'");

            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            while (reader.next(key, value)) {
                writer.append(key, value);
            }

            reader.close();
        }

        writer.close();
    }
    
    /**
     * Hadoop Configuration
     * sources: http://www.slideshare.net/ImpetusInfo/ppt-on-advanced-hadoop-tuning-n-optimisation
     *          https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapred/JobConf.html#setNumReduceTasks(int)
     *          http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.6.0/bk_installing_manually_book/content/rpm-chap1-11.html
     *          http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/TaskConfiguration_H2.html
     */
    
    public static class Machine {
        
        private String machine;
        private int coresPerNode;
        private int memory;
        private int nbNodes;
        private int nbDisks;
        
        private int maximumTasks;
        private int numberReduces;
        private int nContainers;
        private int memoryPerContainer;
        private int mapMemory;
        private int reduceMemory;
        private int mapJavaOpts;
        private int reduceJavaOpts;
        
        public Machine(String machine, int nbNodes) {
            
            this.machine = machine;
            this.nbNodes = nbNodes;
            if (machine.equals("r3.2xlarge")) {
                this.coresPerNode = 8;
                this.memory = 61000;
                this.nbDisks = 1;
            } else if (machine.equals("r3.xlarge")) {
                this.coresPerNode = 4;
                this.memory = 30500;
                this.nbDisks = 1;
            } else if (machine.equals("r3.4xlarge")) {
                this.coresPerNode = 16;
                this.memory = 122000;
                this.nbDisks = 1;
            } else if (machine.equals("gray")) {
                this.coresPerNode = 64;
                this.memory = 256000;
                this.nbDisks = 4;
            } else if (machine.equals("cusp")) {
                this.coresPerNode = 64;
                this.memory = 256000;
                this.nbDisks = 4;
            } else {
                this.coresPerNode = 64;
                this.memory = 256000;
                this.nbDisks = 4;
            }
            
            this.maximumTasks = this.coresPerNode/2;
            this.numberReduces = (int)(1.75*(this.nbNodes*this.maximumTasks));
            this.nContainers = (int) Math.min(2*coresPerNode, Math.min(1.8*nbDisks, (memory-2000)/1000));
            this.memoryPerContainer = (int) Math.max(1000, (memory-2000)/nContainers);
            this.mapMemory = memoryPerContainer;
            this.reduceMemory = 2*memoryPerContainer;
            this.mapJavaOpts = (int) 0.8*memoryPerContainer;
            this.reduceJavaOpts = (int) 0.8*2*memoryPerContainer; 
        }

        public int getMaximumTasks() {
            return maximumTasks;
        }

        public int getNumberReduces() {
            return numberReduces;
        }

        public int getMapMemory() {
            return mapMemory;
        }

        public int getReduceMemory() {
            return reduceMemory;
        }

        public int getMapJavaOpts() {
            return mapJavaOpts;
        }

        public int getReduceJavaOpts() {
            return reduceJavaOpts;
        }
        
        public void setMachineConfiguration(Configuration conf) {
            
            if (machine.equals("r3.2xlarge")) {
                conf.set("yarn.nodemanager.resource.memory-mb", "54272");
                conf.set("yarn.scheduler.minimum-allocation-mb", "3392");
                conf.set("yarn.scheduler.maximum-allocation-mb", "54272");
                conf.set("mapreduce.map.memory.mb", "3392");
                conf.set("mapreduce.reduce.memory.mb", "6784");
                conf.set("mapreduce.map.java.opts", "-Xmx2714m");
                conf.set("mapreduce.reduce.java.opts", "-Xmx5428m");
            } else if (machine.equals("r3.xlarge")) {
                conf.set("yarn.nodemanager.resource.memory-mb", "23424");
                conf.set("yarn.scheduler.minimum-allocation-mb", "2928");
                conf.set("yarn.scheduler.maximum-allocation-mb", "23424");
                conf.set("mapreduce.map.memory.mb", "2982");
                conf.set("mapreduce.reduce.memory.mb", "5856");
                conf.set("mapreduce.map.java.opts", "-Xmx2342m");
                conf.set("mapreduce.reduce.java.opts", "-Xmx4684m");
            } else if (machine.equals("r3.4xlarge")) {
                conf.set("yarn.nodemanager.resource.memory-mb", "116736");
                conf.set("yarn.scheduler.minimum-allocation-mb", "7296");
                conf.set("yarn.scheduler.maximum-allocation-mb", "116736");
                conf.set("mapreduce.map.memory.mb", "7296");
                conf.set("mapreduce.reduce.memory.mb", "14592");
                conf.set("mapreduce.map.java.opts", "-Xmx5837m");
                conf.set("mapreduce.reduce.java.opts", "-Xmx11674m");
            } else if (machine.equals("gray")) {
                conf.set("yarn.nodemanager.resource.memory-mb", "116736");
                conf.set("yarn.scheduler.minimum-allocation-mb", "7296");
                conf.set("yarn.scheduler.maximum-allocation-mb", "116736");
                conf.set("mapreduce.map.memory.mb", "7296");
                conf.set("mapreduce.reduce.memory.mb", "25000");
                conf.set("mapreduce.map.java.opts", "-Xmx5837m");
                conf.set("mapreduce.reduce.java.opts", "-Xmx22000m");
            } else {
                conf.set("yarn.nodemanager.resource.memory-mb", "116736");
                conf.set("yarn.scheduler.minimum-allocation-mb", "7296");
                conf.set("yarn.scheduler.maximum-allocation-mb", "116736");
                conf.set("mapreduce.map.memory.mb", "7296");
                conf.set("mapreduce.reduce.memory.mb", "25000");
                conf.set("mapreduce.map.java.opts", "-Xmx5837m");
                conf.set("mapreduce.reduce.java.opts", "-Xmx22000m");
            }
            
        }
        
    }
    
    /**
     * Exceptions
     */
    
    public static class SmallDatasetException extends Exception {
        private static final long serialVersionUID = 1L;

        public SmallDatasetException(String message) {
            super(message);
        }
    }
    
}
