import numpy as np
import argparse
from sklearn.model_selection import cross_val_score
from sklearn import metrics
from sklearn.metrics import (precision_recall_curve, average_precision_score,
                             roc_curve, roc_auc_score)
                             
from sklearn.ensemble import RandomForestClassifier
from sklearn import svm
import sklearn
import matplotlib.pyplot as plt
import pandas
import math
import operator

def find_optimal_cutoff_closest_to_perfect(fpr, tpr, threshold):
  distance = float("inf")
  threshold_index = -1
  for i in range(len(fpr)):
    d1 = math.sqrt((fpr[i] - 0) * (fpr[i] - 0) + (tpr[i] - 1) * (tpr[i] - 1))
    if d1 < distance:
      distance = d1
      threshold_index = i

  return threshold_index

def analysis_ips(threshold, test_scores, 
                 test_src_ips, test_dest_ips):
  ip_counts = {}
  for i in range(len(test_scores)):
    if test_scores[i] > threshold:
      src_ip = test_src_ips[i]
      dest_ip = test_dest_ips[i]
      if src_ip not in ip_counts:
        ip_counts[src_ip] = 0
      if dest_ip not in ip_counts:
        ip_counts[dest_ip] = 0
      ip_counts[src_ip] += 1
      ip_counts[dest_ip] += 1

  return ip_counts

"""
  Performs the analysis.
  
  Arguments:
  figurenum - Used to increment the figurenum between calls so we don't plot
              different data to the same figure.
  train_y - The training labels.
  train_X - The training data.
  test_y - The test labels.
  test_X - The test data.
  plot - If true, plots the ROC and Precision/Recall curves.
  problem_name - Used to label the plot. 
  test_src_ips - The source ips for the netflows of test set
  test_dest_ips - The dest ips for the netflows of the test set
"""
def analysis(figurenum, 
             train_y, 
             train_X, 
             test_y, 
             test_X, 
             plot, 
             problem_name,
             test_src_ips,
             test_dest_ips):
  clf = RandomForestClassifier()
  clf.fit(train_X, train_y)

  train_scores = clf.predict_proba(train_X)[:,1]
  test_scores = clf.predict_proba(test_X)[:,1]
  train_auc = roc_auc_score(train_y, train_scores)
  test_auc = roc_auc_score(test_y, test_scores)
  train_average_precision = average_precision_score(train_y, train_scores)
  test_average_precision = average_precision_score(test_y, test_scores)
  fpr, tpr, thresholds = roc_curve(test_y, test_scores)
  threshold_index = find_optimal_cutoff_closest_to_perfect(fpr, tpr, thresholds)

  ip_counts = analysis_ips(thresholds[threshold_index], test_scores, 
                           test_src_ips, test_dest_ips)
  sorted_ip_counts = sorted(ip_counts.items(), key=operator.itemgetter(1),
                            reverse=True)
  print( sorted_ip_counts[0:10] )

  print( "AUC of ROC on train", train_auc )
  print( "AUC of ROC on test", test_auc )
  print( "Average Precision on train", train_average_precision )
  print( "Average Precision on test", test_average_precision )
  print( "Optimal point", fpr[threshold_index], tpr[threshold_index])
  if plot:
    precision, recall, thresholds = precision_recall_curve(
                                      test_y, clf.predict_proba(test_X)[:,1])

    figurenum += 1
    f1 = plt.figure(figurenum)
    plt.step(recall, precision, color='b', alpha=0.2, where='post')
    plt.fill_between(recall, precision, step='post', alpha=0.2, color='b')
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.ylim([0.0, 1.05])
    plt.xlim([0.0, 1.0])
    str_average_precision = "{0:.3f}".format(test_average_precision)
    plt.title('Precision-Recall curve of {}: AUC={}'.format(
      problem_name, str_average_precision))
    f1.show()

    figurenum += 1
    f2 = plt.figure(figurenum)
    plt.plot([0,1],[0,1], 'k--')
    plt.plot(fpr, tpr)
    plt.xlabel('False positive rate')
    plt.ylabel('True positive rate')
    str_auc_roc = "{0:.3f}".format(test_auc)
    plt.title('ROC curve of {}: AUC={}'.format(problem_name, str_auc_roc))
    plt.plot([fpr[threshold_index]], [tpr[threshold_index]], marker='o', 
              markersize=10, color="red")
    f2.show()

    return figurenum


def main():

  parser = argparse.ArgumentParser()
  parser.add_argument('--inputfile', type=str, required=True,
                      help="The file with the features and labels.")
  parser.add_argument('--problem_name', type=str,
                      default="SpecifyProblemName",
                      help="The problem name used in plots")
  parser.add_argument('--plot', action='store_true')
  parser.add_argument('--src_ips', type=str, required=True,
                      help="File with list of source ips")
  parser.add_argument('--dest_ips', type=str, required=True,
                      help="File with list of dest ips")
  parser.add_argument('--subset', type=str,
    help="Comma-separated list of features to include") 
                      
  FLAGS = parser.parse_args()

  srcIps = np.loadtxt(FLAGS.src_ips, dtype=np.str)
  destIps = np.loadtxt(FLAGS.dest_ips, dtype=np.str)

  with open(FLAGS.inputfile, "r") as infile:
    data = np.loadtxt(infile, delimiter=",")

    y = data[:, 0] #labels are in the first column
    X = data[:, 1:] #features are in the rest of the columns

    if FLAGS.subset:
      selectedFeatures = FLAGS.subset.split(",")
      selectedFeatures = list(map(int, selectedFeatures))
      X = data[:, selectedFeatures]
      print( X[1] )

    numNonZero = np.count_nonzero(y)
    numFound = 0
    i = 0
    while numFound < numNonZero/2:
      if y[i] == 1:
        numFound += 1
      i += 1
    
    i -= 1
    y1 = y[0: i]
    y2 = y[i:]
    X1 = X[0:i]
    X2 = X[i :]
    print( "Length y1, y2", len(y1), len(y2) )
    print( "Share X1, X2", X1.shape, X2.shape )
    print( "Nonzero y1, y2", np.count_nonzero(y1), np.count_nonzero(y2) )
    figurenum = 0
    figurenum = analysis(figurenum, y1, X1, y2, X2, FLAGS.plot, 
                          FLAGS.problem_name, srcIps[i:], destIps[i:])
    figurenum = analysis(figurenum, y2, X2, y1, X1, FLAGS.plot, 
                          FLAGS.problem_name, srcIps[0:i], destIps[0:i])


    if FLAGS.plot:
      input()

main()
