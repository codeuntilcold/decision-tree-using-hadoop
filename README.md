# Mapreduce Implementation of Decision tree

Decision tree is a type of supervised machine learning algorithm that is most commonly used in data mining. They can be used to generate trees or rule sets depending on the implementation, that can then be used to perform predictions.

MapReduce is a programming model for processing and generating big data sets with a parallel, distributed algorithm on a cluster. A MapReduce program is composed of a map procedure, which performs filtering and sorting, and a reduce method, which performs a summary operation. The major advantage of Mapreduce is that the programs can scale horizontally by adding additional computing hardware.

Our program is an implementation of a decision tree algorithm that uses Information gain based on entropy of a possible split attribute. The program is structured into a series of map and reduce steps and can be run on mapreduce implementations such as Apache Hadoop, Google Dataproc or Amazon EMR.

_The program can be run on various platforms, or on a local machine using a single thread, or even in a simulated local cluster. All these can be easily changed with a configuration option._

## Dependency installation

-   Python2.7: The program needs python2.7 to run and this can be installed in a straightforward manner. Installers for Windows and MacOS can be found [here](https://www.python.org/downloads/release/python-2716/). For linux distributions, the installation can be done using the default package manager. The instructions for Ubuntu are shown below.
    ```
    sudo apt-get update
    sudo apt-get install python2
    ```
-   Pip for python2.7: In case of Windows and MacOS, pip should be available by default when python2.7 is installed. However, for linux, the package will have to be installed separately. In any case, the following instructions can be used to install pip for python2.7

    -   Make sure you have the `get-pip.py` file (present in the root directory)
    -   Run it using python2.7. Run it with `sudo` if it is to be installed in the root user

    This will install pip for python2.7 in the directory where python is installed.

-   Installing mrjob
    ```
    python2 -m pip install mrjob
    ```
    The python2 in the command is better replaced with the complete python2.7 installation path to avoid any ambiguities

## Running the program

-   Set the dataset details in main.py. The default dataset being used is the playtennis.txt
-   The dataset must be comma separated and not have a header row
-   Set the runner being used. By default, the program uses an inline runner, that runs the program sequentially in a single thread. Several other values are available. Some of them are:
    -   inline: Runs the process in a single thread sequentially
    -   local: Simluates a hadoop cluster in the local machine using differnt threads as worker nodes
    -   hadoop: The program is given to a hadoop cluster tto run. Assumes that the program is being run in a hadoop cluster and that all the nodes have python installed
-   In case the program is to be run in a hadoop cluster, the cluster will have to be set up. This can be done in several ways:
    -   Install hadoop cluster in a docker environment. This can be done using the [Big Data Hadoop repository](https://github.com/big-data-europe/docker-hadoop). The _docker-compose up_ command can be used to spin up a docker environment. However, python (and mrjob) will have to be installed in theh cluster manually
    -   Use a service such as Google Dataproc or Amazon EMR. Here, mrjob will have to be installed on the namenode of the hadoop cluster. It is to be noted that this may charge money if the free tier is not available
-   After running the program, a `rules.txt` file will be generated that will contain the rulesets generated from the dataset

Note: The program was written using python2.7 for compatibility reasons in Google Dataproc and the code can be converted to python3 with minimal changes.
