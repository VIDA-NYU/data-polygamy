# Data Polygamy

Data Polygamy is a scalable topology-based framework that allows users to query for statistically significant relationships between spatio-temporal datasets. For more detailed information about the our framework, please refer to our SIGMOD paper:

*Data Polygamy: The Many-Many Relationships among Urban Spatio-Temporal Data Sets, F. Chirigati, H. Doraiswamy, T. Damoulas, and J. Freire. In Proceedings of the 2016 ACM SIGMOD International Conference on Management of Data (SIGMOD), 2016*

We strongly suggest users to read our paper before using our code.

The team includes:

* [Fernando Chirigati][fc] (New York University)
* [Harish Doraiswamy][hd] (New York University)
* [Theodoros Damoulas][td] (University of Warwick)
* [Juliana Freire][jf] (New York University)

[fc]: http://bigdata.poly.edu/~fchirigati/
[hd]: http://www.harishd.com/
[td]: http://www2.warwick.ac.uk/fac/sci/statistics/staff/academic-research/damoulas
[jf]: http://vgc.poly.edu/~juliana/

Our code and data are this repository is available under the [BSD](LICENSE) license.

## Contents

This README file is divided into the following sections:

* [Repository Overview](#repository-overview)
* [Dependencies](#dependencies)
* [Preliminaries](#preliminaries)
    * [HDFS Directory](#hdfs-directory)
    * [Spatial Resolutions](#spatial-resolutions)
    * [Data](#data)
* [How To Build](#how-to-build)
* [How To Run](#how-to-run)
    * [Common Arguments](#common-arguments)
    * [Pre-Processing Step](#pre-processing-step)
    * [Step 1: Scalar Function Computation](#step-1-scalar-function-computation)
    * [Step 2: Feature Identification](#step-2-feature-identification)
    * [Step 3: Relationship Computation (Query Evaluation)](#step-3-relationship-computation-query-evaluation)
    * [Alternate Step: Correlation Computation](#alternate-step-correlation-computation)
* [Experiments](#paper-experiments)

## Repository Overview

Soon ...

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

## Preliminaries

This section describes information about the data used by the framework that *must* be in place before executing the framework.

### HDFS Directory

The code originally reads from and writes to HDFS. It assumes that the HDFS home directory has the following structure:

    .
    +-- data/
    |   +-- datasets
    |   +-- dataset
    |   +-- dataset.header
    |   +-- dataset.defaults
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

* **``data``** is a directory containing all the datasets and metadata associated to the datasets (more information [later](#data));
* **``pre-processing/``** is a directory that stores the results from the [pre-processing step](#pre-processing-step);
* **``aggregates/``** is a directory that stores the results from the [scalar function computation step](#step-1-scalar-function-computation);
* **``index/``** is a directory that stores the results from the [feature identification step](#step-2-feature-identification);
* **``mergetree/``** is a directory that stores the previously computed merge trees;
* **``relationships/``** is a directory that stores the results from the [relationship computation step](#step-3-relationship-computation-query-evaluation), i.e., the topology-based relationships between datasets;
* **``relationships-ids/``** is a directory similar to ``relationships/``, but the relationships are stored with the datasets ids, instead of the datasets names;
* **``correlations/``** is a directory that stores the results from the standard correlation techniques step;
* **``neighborhood``** and **``zipcode``** are files that contain the polygons corresponding to the neighborhood and zipcode resolutions, respectively (more information [later](#spatial-structures));
* **``neighborhood-graph``** and **``zipcode-graph``** are files that contain the graph structure of the neighborhood and zipcode resolutions, respectively (more information [later](#spatial-structures)).

To automatically create the required directories, take a look at the [``load-hdfs-structure``](data/load-hdfs-structure) script.

### Spatial Resolutions

The current implementation of Data Polygamy has support to five spatial resolutions: *GPS*, *neighborhood*, *zipcode*, *grid*, and *city*. The grid resolution has only been used for testing, and not in our final experiments. Note that the framework assumes that all the data fed to the pre-processing step corresponds to a single city; therefore, if you are handling data from more than one city, you probably need to provide a suitable resolution conversion under the [``resolution``](data-polygamy/src/main/java/edu/nyu/vida/data_polygamy/resolution/) directory.

To use the neighborhood and zipcode resolutions, two files must be provided for each: a **polygons** file, and a **graph** file. The former contains all the polygons that represent the different regions of the resolution (e.g.: neighborhoods or zipcodes) with their corresponding ids. A polygon, in this case, is represented by a set of GPS points, where the last point is the same as the first one. The format is the following:

    <region-id>              # first region
    <number-of-polygons>
    <number-of-data-points>  # first polygon
    <point-1>
    <point-2>
    ...    
    <number-of-data-points>  # second polygon
    ...
    <region-id>              # second region
    ...

The files [``neighborhood.txt``](data/neighborhood.txt) and [``zipcode.txt``](data/zipcode.txt) are examples of such file for New York City.

The **graph** file represents a graph for the resolution, where each region of the resolution is a node, and there is an edge between two regions if these are neighboring regions. The first line of this file contains the number of nodes and number of edges, and the following lines represent the edges of the graph (one line per edge). The files [``neighborhood-graph.txt``](data/neighborhood-graph.txt) and [``zipcode-graph.txt``](data/zipcode-graph.txt) are examples of such file for New York City.

The script [``load-spatial``](data/load-spatial) can be used to automatically upload our spatial resolutions files to HDFS. 

### Data

The ``data`` directory under HDFS contains all the datasets used by the framework.

#### Dataset Attributes

We assume the following types of attributes for a dataset:

* **Spatial attributes** represent the spatial component of the data (e.g.: a GPS point, or a neighborhood region).
* **Temporal attributes** represent the temporal component of the data (e.g.: a GPS point, or a neighborhood region). Such attribute *must* have values that represent the number of seconds since Epoch time, in UTC.
* **Identifier attributes** represent unique identifiers for the dataset (e.g.: Twitter user id, or taxi medallion). The header for these attributes *must* contain either *id*, *name*, or *key*.
* **Numerical attributes** represent the real-valued attributes of the datasets, which are the attributes of interest for the relationships.

All the other attributes can be ignored by enclosing their values by either double quotes (e.g.: ``"ignore me!"``) or the symbol `$` (e.g.: ``$ignore me!$``). Alternatively, you can also simply delete these attributes before executing the framework.

#### Dataset Files

For each dataset, three files are required and must be located under the ``data`` directory. For the purpose of this documentation, assume a dataset named *taxi*:

* **``taxi``**: a CSV file containing the data corresponding to the dataset (without any headers).
* **``taxi.header``**: a CSV file containing a single line, which is the header of the dataset.
* **``taxi.defaults``**: a CSV file with a single line containing the default values for each attribute of the dataset. If an attribute does not have a default value, ``NONE`` should be used. Note that default values are *ignored* by the framework.

In addition to these dataset files, a file named ``datasets`` must be created under the ``data`` directory, containing a mapping between dataset name and dataset id. An example of such file is available [here](data/datasets.txt).

## How To Build

We use [Apache Maven](https://maven.apache.org/) to build the Data Polygamy framework:

    $ cd data-polygamy/
    $ mvn clean package

This generates a jar file, with the following name and path: ``data-polygamy/target/data-polygamy-0.1-jar-with-dependencies.jar``. For simplicity, we refer to this file as ``data-polygamy.jar`` throughout this documentation. 

Note that all the dependencies are taken care of by Maven, except for [JIDT](http://jlizier.github.io/jidt/), [Java-ML](http://java-ml.sourceforge.net/), and [JavaMI](http://www.cs.man.ac.uk/~pococka4/JavaMI.html), since these libraries are not available in the central repository. Therefore, we include these libraries, as well as their corresponding licenses, under [``data-polygamy/lib``](data-polygamy/lib). It is worth mentioning that we **did not** make modifications to any of these libraries.

## How To Run

To run our framework, you will need [Apache Hadoop](http://hadoop.apache.org/). The framework can be summarized as follows:

<img src="framework.png" height="125">

Each step of the framework is represented by a map-reduce job. The Pre-Processing step is executed once for each dataset, while the other steps can be executed once for multiple datasets.

### Common Arguments

The following command-line arguments are available in all the steps of the framework:

*Required Arguments*:

* **``-m``**: identifier for the machine configuration of the Hadoop cluster nodes. These identifiers are defined in the class Machine in file [FrameworkUtils.java](data-polygamy/src/main/java/edu/nyu/vida/data_polygamy/utils/FrameworkUtils.java). For each identifier, information related to the corresponding machine is declared (e.g.: number of cores, amount of memory, and number of disks). Such information is used to set a few Hadoop configuration parameters.
* **``-n``**: number of nodes in the Hadoop cluster.

*Optional Arguments*:

* **``-f``**: flag that forces the execution of the step for all the input datasets, even if the results for these datasets already exist. In other words, existing results or output files are deleted and the step is re-executed for all input datasets.
* **``-s3``**: flag that indicates that the execution will read data from and write data to a bucket on [Amazon S3](https://aws.amazon.com/s3/) storage service.
* **``-aws_id``**: the AWS Access Key Id. This argument is required if the s3 flag is used.
* **``-aws_key``**: the AWS Secret Access Key. This argument is required if the ``s3`` flag is used.
* **``-b``**: the bucket on S3 where data will be read from and write to. This argument is required if the ``s3`` flag is used.
* **``-h``**: flag that displays a help message.

### Pre-Processing Step

The Pre-Processing step is responsible for selecting data (from a dataset) that correspond to spatial, temporal, identifier, and numerical [attributes](#dataset-attributes). This step also does a pre-aggregation that is fed to the scalar function computation step.

To run the pre-processing step:

    $ hadoop jar data-polygamy.jar edu.nyu.vida.data_polygamy.pre_processing.PreProcessing -m <machine> -n <number-nodes> -dn <dataset name> -dh <dataset header file> -dd <dataset defaults file> -t <temporal resolution> -s <spatial resolution> -cs <current spatial resolution> -i <temporal index> <spatial indices> ...
    
where:

* **``-dn``** indicates the dataset name, which should match the dataset file under the ``data`` directory.
* **``-dh``** indicates the dataset header file, located under the ``data`` directory.
* **``-dd``** indicates the dataset defaults file, located under the ``data`` directory.
* **``-t``** indicates the minimum temporal resolution that this data should be aggregated. This can take the following values: *hour*, *day*, *week*, or *month*. We recommend setting this to *hour*.
* **``-cs``** indicates the current spatial resolution of the data, i.e., *points* (for GPS points), *nbhd* (for neighborhood), *zip* (for zipcode), or *city*.
* **``-s``** indicates the minimum spatial resolution that this data should be aggregated. For instance, if the current spatial resolution is *points*, the minimum spatial resolution could be *nbhd* or *zip*.
* **``-i``** indicates the indices for the temporal and spatial attributes. For instance, if the temporal attribute is on index 0, and the x and y components of the spatial attribute is on indices 2 and 3, this should be set as ``-i 0 2 3``.

The results are stored under the ``pre-processing`` directory.

### Step 1: Scalar Function Computation

The Scalar Function Computation step is responsible for generating all possible scalar functions at different spatio-temporal resolutions.

To run the scalar function computation step:

    $ hadoop jar data-polygamy.jar edu.nyu.vida.data_polygamy.scalar_function_computation.Aggregation -m <machine> -n <number-nodes> -g <datasets>
    
where:

* **``-g``** indicates the datasets for which the scalar functions will be computed, followed by the temporal and spatial indices. For instance, to execute this step for the taxi and 311 datasets, one can use ``-g taxi 0 0 311 0 0``.

The results are stored under the ``aggregates`` directory.

### Step 2: Feature Identification

The Feature Identification step creates the merge tree indices (if they have not been created yet) and identifies the set of features for the different scalar functions.

To run the feature identification step:

    $ hadoop jar data-polygamy.jar edu.nyu.vida.data_polygamy.feature_identification.IndexCreation -m <machine> -n <number-nodes> -g <datasets> -t
    
where:

* **``-g``** indicates the datasets for which the features will be identified and computed (e.g.: ``-g taxi 311``).
* **``-t``** is an *optional* flag that indicates that this step should use custom thresholds for salient and extreme features, instead of relying on our data-driven approach. Custom thresholds must be written to a file named ``data/thresholds``.

The format of file ``data/thresholds`` must be the following:

    <dataset-name>
    <scalar-function-id> <threshold-salient-feature> <threshold-extreme-feature>
    <scalar-function-id> <threshold-salient-feature> <threshold-extreme-feature>
    ...
    <dataset-name>
    <scalar-function-id> <threshold-salient-feature> <threshold-extreme-feature>
    <scalar-function-id> <threshold-salient-feature> <threshold-extreme-feature>
    ...
    
In this file, values in a line are separated by the tab character (i.e., ``\t``). To know which scalar function ids to use, you can take a look at the file ``pre-processing/*.aggregates`` corresponding to the dataset of interest.

The results (set of features for each scalar function at different resolutions) are stored under the ``index`` directory. Merge tree indices are stored under the ``mergetree`` directory.

### Step 3: Relationship Computation (Query Evaluation)

The Relationship Computation step evaluates the relationships between all the possible pairs of functions corresponding to the input query, i.e., the query evaluation happens in this step.

To run the relationship computation step:

    $ hadoop jar data-polygamy.jar edu.nyu.vida.data_polygamy.relationship_computation.Relationship -m <machine> -n <number-nodes> -g1 <datasets> -g2 <datasets> -sc <score-threshold> -st <strength threshold> -c -id -r
    
where:

* **``-g1``** is the first group of datasets.
* **``-g2``** is the *optional* second group of datasets.
* **``-sc``** is an *optional* threshold for relationship score.
* **``-st``** is an *optional* threshold for relationship strength.
* **``-c``** is an *optional* flag for using complete, rather than restricted, randomization for the Monte Carlo tests.
* **``-id``** is an *optional* flag for returning dataset ids, instead of dataset names, in the relationship results.
* **``-r``** is an *optional* flag that indicates that relationships that are identified as not significant should be removed from the final output.

This step supports the general form of the *relationship query*:

<b><i>Find relationships between G1 and G2 satisfying CLAUSE.</i></b>

*G1* and *G2* are the groups of datasets corresponding to arguments ``-g1`` and ``-g2``: all the possible relationships between *G1* and *G2* are evaluated; if *G2* is not provided, we assume that *G2* encompasses all the datasets in the corpus (i.e., under the ``data`` directory), thus allowing hypothesis generation. The remaining arguments and flags are part of the *CLAUSE* sentence. If users want to specify custom thresholds for computing salient and extreme features, instead of doing so as part of the *CLAUSE* sentence, it is better to first re-execute [the feature identification step](#step-2-feature-identification) (specifying the desired thresholds), and then execute the relationship computation step.

The results are stored under the ``relationships`` directory if flag ``-id`` is not used; otherwise, results are stored under the ``relationships-ids`` directory.

### Alternate Step: Correlation Computation

<img src="framework-standard-techniques.png" height="110">

## Experiments

* Java 1.7.0_45
* [Apache Maven](https://maven.apache.org/) 3.3.9
* [Apache Hadoop](http://hadoop.apache.org/) 2.2.0

More details soon...

