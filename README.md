<h1>Mathio.js - big math operations on cloud</h1>

<h2>Introduction</h2>
============

Mathio.js is a front-end for simple math operations running on large data sets. A client-service model is offered to users
 who want to perform large computations (such as chained matrix multiplication) in information retrieval, machine learning 
 and other fields.
 
 The software is built on a service model. Users can download the software and run a server application that hosts the
 computational logic. Clients can be written using a simple command-line interface that talks to the server over HTTP.
 
 Node.js uses the following open-source frameworks/software:
 
 (1) Node.js - the server and client are built on the Node.js framework
 (2) Hadoop - computations are run on Hadoop in local/hdfs/s3 modes
 (3) Python - random test generators for math features
 (4) Amazon AWS - computations can be run locally or use the Amazon Elastic Map-Reduce framework. Sample AWS scripts are
 provided for running/tracking jobs. These scripts are built on top of the Ruby client.
 
 <h2>Hierarchy</h2>
 =========
 
 <root>
 |
 ----- /server (server scripts)
 ----- /client (example clients, users can modify to suit their needs)
 ----- /aws (aws scripts for tracking/running/stopping EMR jobs)
 ----- /gen (random data generators)
 ----- /target (output of build - jar,class etc.)
 ----- /src (source code)
 ----- pom.xml (main build script)
 
<h2>Requirements</h2>
============

Hadoop 2.x+
Node.js 0.10.x+
Python 2.7+ (may work for earlier versions but not tested)
Java 1.6.x+ 
OS x86_64 (may work for other platforms but not tested)

<h2>Installation</h2>
============

From the root directory:

mvn package

<h2>Running mathio.js<h2>
=================

Please make sure the following are installed:
1. Set environment variable MATHIOJS_HOME to the root directory
2. cd $MATHIOJS_HOME/server
   node index
   
   Now we are ready to run the client.
   
3. For example clients, please cd to $MATHIOJS_HOME/client
   There are examples for local/hdfs/s3 modes
   

   


