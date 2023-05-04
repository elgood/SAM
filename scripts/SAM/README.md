# KMedoids

K-Medoids is a clustering algorithm that is similar to KMeans
but uses the median instead of the mean to calculate
cluster centers. We are using the SAM program to analyze a
dataset and group similar data points into clusters. Our goal
is to evaluate the performance of the K-Medoids algorithm
on this dataset and compare it to other clustering algorithms.

This folder contains independent files with the code implementation in "kmedoids_clustering", 
with the hope that this will be integrated directly into SAM one day.

Use CTU Scenario training data to test the implementation.

### TODO: 
- Remove any dependencies on pre-existing feature files, run this directly from within SAM using features generated 
by the application
- Update Unit Tests accordingly
- Restructure and integrate into SAM

### How To Run:
- Ensure that you change your working directory to where the feature file(s) is stored.

- Usage: python kmedoids_clustering.py <data_file>

- To run unit tests: python test_kmedoids_clustering.py
