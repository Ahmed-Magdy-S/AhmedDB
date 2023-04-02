# AhmedDB
Fully functional relational database management system which is designed and implemented in Java. 

## Introduction
The project for learning purpose to get conceptual knowledge and experience about how a database management system works under the hood and how every component interacts with each other.
Its implementation is similar to SimpleDb which created by Edward Sciore in its [book](https://link.springer.com/book/10.1007/978-3-030-33836-7) but with modified APIs to be more understandable with extended features and appropriate documentation comments.

## Features

- Functionally, it is a multiuser, transaction-oriented database server that executes SQL statements and interacts with clients via JDBC.
- Structurally, it contains the same basic components as a commercial database management system, with similar APIs.


### Implementation Roadmap
- [x] Disk and File Management
  - Added FileManager class (handling the actual interaction with the OS file system).
  - Added LogicalBlock class (handling block numbers in a specific disk file).
  - Added Page class (act as a holder for block contents in a specific size of memory. Used for both actual data and log data).
- [x] Memory Management
  - Added LogManager class (which is responsible for writing log records into a log file).
  - Added LogIterator class (provides the ability to move through the records of the log file).
- [x] Transaction Management
  1. Recovery Management
     - Added log record classes for helping in recovery management by recording transaction processes.
  2. Concurrency Management
- [ ] Record Management
- [ ] Metadata Management
- [ ] Query Processing
- [ ] Parsing
- [ ] Planning Process
- [ ] JDBC Interfaces
- [ ] Indexing
- [ ] Materialization and Sorting
- [ ] Effective Buffer Utilization
- [ ] Query Optimization

## Setup
It will be available after finishing of the project.