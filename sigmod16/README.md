## Readme for reproducibility submission of paper ID 982

Please see our main [README file](https://github.com/ViDA-NYU/data-polygamy/blob/master/README.md) for more details.

### A) Source code info

* Repository: [https://github.com/ViDA-NYU/data-polygamy](https://github.com/ViDA-NYU/data-polygamy)
* Programming Languages: Java (for main source code) and Python (for experiments and plots)
* Additional Programming Language info: Java version 1.7.0_45, and Python version 2.7.3
* Compiler Info: Apache Maven 3.3.9
* Packages/Libraries Needed: [Dependencies](https://github.com/ViDA-NYU/data-polygamy#2-dependencies)

### B) Datasets info

* Repository: [Datasets](https://github.com/ViDA-NYU/data-polygamy#62-datasets)
* Data generators: There are no data generators, but we provide scripts to download and configure all our data and metadata; please see the [``prepareData.sh``](sigmod16/prepareData.sh) script.

### C) Hardware Info

Most of the experiments were executed on a cluster with 20 compute nodes, each node running Red Hat Enterprise Linux Server release 6.7, and having the following configuration: an  and 256GB of RAM.

* Processor: AMD Opteron(TM) Processor 6272 (4x16 cores) 2.1GHz
* Caches: 3 levels; level 1: 8 x 64KB; level 2: 8 x 2MB; level 3: 2 x 8MB
* Memory: 256GB
* Secondary Storage: IBM DCS3700; capacity of 505.086TB of 360 SAS disks
* Network: IBM Networking Operating System RackSwitch G8264 10GBe

The scalability experiment was the only one performed on Amazon Web Services (AWS), and more information about it is available [here](https://github.com/ViDA-NYU/data-polygamy#scalability-figure-10). We provide scripts to automatically setup the cluster.

### D) Experimentation Info

* Scripts and how-tos to generate all necessary data or locate datasets: please see the [``prepareData.sh``](sigmod16/prepareData.sh) script and sections [6.2](https://github.com/ViDA-NYU/data-polygamy#62-datasets) and [6.3](https://github.com/ViDA-NYU/data-polygamy#63-initial-setup).
* Scripts and how-tos to prepare the software for system: please see the [``prepareSoftware.sh``](sigmod16/prepareSoftware.sh) script and section [4](https://github.com/ViDA-NYU/data-polygamy#4-how-to-build).
* Scripts and how-tos for all experiments executed for the paper: please see the [``runExperiments.sh``](sigmod16/runExperiments.sh) script and section [6](https://github.com/ViDA-NYU/data-polygamy#6-experiments).