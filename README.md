# In Construction...

# Data Polygamy

Data Polygamy is a scalable topology-based framework that allows users to query for statistically significant relationships between spatio-temporal data sets.

This README file is divided into the following sections:

* [Links and References](#links-and-references)
* [Dependencies](#dependencies)
* [Repository Overview](#repository-overview)
* [How To Build](#how-to-build)
* [How To Run](#how-to-run)
    * [Preliminaries](#preliminaries)
    * [Common Arguments](#common-arguments)
    * [Pre-Processing Step](#pre-processing-step)
    * [Step 1: Scalar Function Computation](#step-1-scalar-function-computation)
    * [Step 2: Feature Identification](#step-2-feature-identification)
    * [Step 3: Relationship Computation](#step-3-relationship-computation)
* [Paper Experiments](#paper-experiments)

## Links and References

For more detailed information about the our framework, please refer to our SIGMOD paper:

*Data Polygamy: The Many-Many Relationships among Urban Spatio-Temporal Data Sets, F. Chirigati, H. Doraiswamy, T. Damoulas, and J. Freire. In Proceedings of the 2016 ACM SIGMOD International Conference on Management of Data (SIGMOD), 2016*

The team includes:

* [Fernando Chirigati][fc] (New York University)
* [Harish Doraiswamy][hd] (New York University)
* [Theodoros Damoulas][td] (University of Warwick)
* [Juliana Freire][jf] (New York University)

[fc]: http://bigdata.poly.edu/~fchirigati/
[hd]: http://www.harishd.com/
[td]: http://www2.warwick.ac.uk/fac/sci/statistics/staff/academic-research/damoulas
[jf]: http://vgc.poly.edu/~juliana/

Our code and data is available under the [BSD](LICENSE) license.

## Dependencies

The Data Polygamy framework uses Java 1.7.0_45 and has the following dependencies:

* [Apache Commons Lang 2.6](http://commons.apache.org/proper/commons-lang/)
* [Apache Commons CLI 1.2](http://commons.apache.org/proper/commons-cli/)
* [Apache Commons Collections 3.2.1](http://commons.apache.org/proper/commons-collections/)
* [Apache Commons CSV 1.0](http://commons.apache.org/proper/commons-csv/)
* [Apache Commons Logging 1.2](http://commons.apache.org/proper/commons-logging/)
* [Apache Commons Math 3.3](https://commons.apache.org/proper/commons-math/)
* [fastutil 6.6.5](http://fastutil.di.unimi.it/)
* [Guava 18.0](https://github.com/google/guava)
* [Apache Hadoop Annotations 2.4.0](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-annotations/2.4.0)
* [Apache Hadoop Common 2.4.0](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common/2.4.0)
* [Hadoop Mapreduce Client Core 2.4.0](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core)
* [Joda-Time 2.5](http://www.joda.org/joda-time/)
* [JIDT 1.3](http://jlizier.github.io/jidt/)
* [Java-ML 0.1.7](http://java-ml.sourceforge.net/)
* [JavaMI 1.0](http://www.cs.man.ac.uk/~pococka4/JavaMI.html)

The plots ...

## Repository Overview



## How To Build

We use [Apache Maven](https://maven.apache.org/) 3.3.9 to build the Data Polygamy framework:

    $ cd data-polygamy/
    $ mvn clean package

This generates a jar file, with the following name and path: ``data-polygamy/target/data-polygamy-0.1-jar-with-dependencies.jar``. For simplicity, we refer to this file as ``data-polygamy.jar`` throughout this documentation. 

Note that all the dependencies are taken care of by Maven, except for [JIDT](http://jlizier.github.io/jidt/), [Java-ML](http://java-ml.sourceforge.net/), and [JavaMI](http://www.cs.man.ac.uk/~pococka4/JavaMI.html), since these libraries are not available in the central repository. Therefore, we include these libraries, as well as their corresponding licenses, under [``data-polygamy/lib``](data-polygamy/lib). It is worth mentioning that we **did not** make modifications to any of these libraries.

## How To Run

To run the different steps of the framework, you will need [Apache Hadoop](http://hadoop.apache.org/).

We strongly suggest users to read our [paper](#Links) before using our code.

### Preliminaries

The code assumes that the HDFS has the following structure:

    .
    +-- data/
    |   +-- datasets
    |   +-- ...
    +-- pre-processing/
    |   +-- ...
    +-- aggregates/
    |   +-- ...
    +-- index/
    |   +-- ...
    +-- mergetree/
    |   +-- ...
    +-- relationships/
    |   +-- ...
    +-- relationships-ids/
    |   +-- ...
    +-- correlations/
    |   +-- ...
    +-- neighborhood
    +-- neighborhood-graph
    +-- zipcode
    +-- zipcode-graph

where:

* **``pre-processing/``** is a directory that stores the results from the pre-processing step;
* **``aggregates/``** is a directory that stores the results from the scalar function computation step;
* **``index/``** is a directory that stores the results from the feature identification step;
* **``mergetree/``** is a directory that stores the previously computed merge trees;
* **``relationships/``** is a directory that stores the results from the relationship computation step, i.e., the topology-based relationships between data sets;
* **``relationships-ids/``** is a directory similar to ``relationships/``, but the relationships are stored with the data sets ids, instead of the data sets names;
* **``correlations/``** is a directory that stores the results of standard correlation techniques (Pearson's correlation, mutual information, and DTW) when applied to the data sets;
* **neighborhood** and **zipcode** are files that contain the polygons corresponding to the neighborhood and zipcode resolutions, respectively;
* **neighborhood-graph** and **zipcode-graph** are files that contain the graph structure of the neighborhood and zipcode resolutions, respectively.

Directory structure, data sets, header files, defaults files, ...

### Common Arguments

The following command-line arguments are available in all the steps of the framework:

*Required Arguments*:

* **-m**: identifier for the machine configuration of the Hadoop cluster nodes. These identifiers are defined in the class Machine in file [FrameworkUtils.java](data-polygamy/src/main/java/edu/nyu/vida/data_polygamy/utils/FrameworkUtils.java). For each identifier, information related to the corresponding machine is declared (e.g.: number of cores, amount of memory, and number of disks). Such information is used to set a few Hadoop configuration parameters.
* **-n**: number of nodes in the Hadoop cluster.

*Optional Arguments*:

* **-f**: flag that forces the execution of the step for all the input data sets, even if the results for these data sets already exist. In other words, existing results or output files are deleted and the step is re-executed for all input data sets.
* **-s3**: flag that indicates that the execution will read data from and write data to a bucket on [Amazon S3](https://aws.amazon.com/s3/) storage service.
* **-aws_id**: the AWS Access Key Id. This argument is required if the **s3** flag is used.
* **-aws_key**: the AWS Secret Access Key. This argument is required if the **s3** flag is used.
* **-b**: the bucket on S3 where data will be read from and write to. This argument is required if the **s3** flag is used.
* **-h**: flag that displays a help message.

### Pre-Processing Step

The Pre-Processing step is responsible for selecting data (from a data set) that correspond to spatial, temporal, identifier, and numerical attributes. This step also does a pre-aggregation that is fed to the scalar function computation step.

To run the pre-processing step, run:

    $ hadoop jar data-polygamy.jar edu.nyu.vida.data_polygamy.pre_processing.PreProcessing -dn <data set name> -dh <data set header file> -dd <data set defaults file> -t <temporal resolution> -s <spatial resolution> -cs <current spatial resolution> -i <temporal index> <spatial indices> ...
    
where: 

### Step 1: Scalar Function Computation

### Step 2: Feature Identification

### Step 3: Relationship Computation

## Paper Experiments


