Investigating the Lambda-Architecture
=====================================
Master Thesis at the University of Zurich

# Requirements
* Maven
* Java 7
* Pytohn 3


# Dependencies
* Java dependencies are managed with maven
    *  Samza and Pig were patched and are provided in the lib directory
* Pyhton requirements can be found here: [requirements.pip](automatic_deployment/src/requirements.pip)


# Code Structure
The batch, speed and coordination layers are in their corresponding packages. The automatic_deployment comes in two flavors:
(i) a local version and (ii) a distributed version that runs on SLURM or TORQUE. The data module includes common data structures
such as objects to represent the data sets (DEBS and SRBench) and the esper queries that are used in the batch and speed layer.
The utils package includes helper tools for example to partition the data or to inspect the Kafka topics.


# Local Deployment
The local deployment script installs all services on the local node and can start and stop these services. The script `grid.sh`
can be called to see all options. Note that the install functionality depends on the download links of these products and
these links will change in the future. Therefore you may have to download some services manually.


# Cluster Deployment
The automatic deployment script is designed to start experiments on a SLURM or TORQUE cluster. All scripts are located
in the same directory `execute`. The reason to not further modularize the scripts is the burden to deal with multiple
directory in a cluster environment. Tools such as pbsdsh or srun may impose strange constraints to switch between directories.

`start_all` is the script that encapsulates the logic to run experiments (e.g. `sbatch start_all.sh`). It is a good starting point
to understand how the cluster setup is done and what parameters can be applied.

# Remarks
Please note the directory `/speed/src/main/java/ch/uzh/thesis/lambda_architecture/speed/spout/kafka` includes code from the 
Storm repository. It was not possible to alter the behavior of the KafkaSpout through inheritance. Therefore the code 
was copied and modified. For a reference see (Storm)[https://github.com/apache/incubator-storm/tree/master/external/storm-kafka]