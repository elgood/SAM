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

def load_dataset(inputfile):
  with open(inputfile, "r") as infile:
    data = np.loadtxt(infile, delimiter=",")

    y = data[:, 0] #labels are in the first column
    X = data[:, 1:] #features are in the rest of the columns

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

    return X1, X2, y1, y2

def train(trainx, trainy, testx, testy, selected, feature):
    #print("trainx.shape", trainx.shape, "trainy.shape", trainy.shape)
    #print(list(selected))
    feature_list = list(selected)
    feature_list.append(feature)
    print("feature_list", feature_list)
    trainx = trainx[:, feature_list]
    testx = testx[:, feature_list]

    clf = RandomForestClassifier()
    print("trainx.shape", trainx.shape, "trainy.shape", trainy.shape)
    clf.fit(trainx, trainy)
    test_scores = clf.predict_proba(testx)[:,1]
    auc = roc_auc_score(testy, test_scores)
    return auc

def find_best(selected, available, X1s, X2s, y1s, y2s):
  
  best = 0
  best_feature = -1

  for feature in available:
    aucs = []
    for i in range(len(X1s)):
      auc1 = train(X1s[i], y1s[i], X2s[i], y2s[i], selected, feature)
      print("auc1", auc1)
      auc2 = train(X2s[i], y2s[i], X1s[i], y1s[i], selected, feature)
      print("auc2", auc2)
      aucs.append(auc1)
      aucs.append(auc2)
    average = sum(aucs) / len(aucs)
    print ("average", average)
    if average > best:
      print("updating best")
      best = average
      best_feature = feature 

  return best_feature, best 

def main():

  parser = argparse.ArgumentParser("You specify a directory for where" +
    " the CTU dataset is located.")
  parser.add_argument('--dir', type=str, required=True,
                      help="The directory where the CTU stuff is.")
  parser.add_argument('--num_ctu', type=int, default=13,
    help="The number of CTU directories to use.")
  parser.add_argument('--num_features', type=int, default=28,
    help="The number of features to explore.")
                      
  FLAGS = parser.parse_args()
  X1s = []
  X2s = []
  y1s = []
  y2s = []
  for i in range(FLAGS.num_ctu):
    filename = FLAGS.dir + "/" +  str(i + 1) + "/simple_features_dest_src.csv"
    print ("Loading " + filename )
    X1, X2, y1, y2 = load_dataset(filename)
                                  
    X1s.append(X1)
    X2s.append(X2)
    y1s.append(y1)
    y2s.append(y2)

  selected = set()
  available = set()
  aucs = []
  for i in range(FLAGS.num_features):
    available.add(i)

  for i in range(FLAGS.num_features):
    best_feature, auc = find_best(selected, available, X1s, X2s, y1s, y2s)
    print ("Adding feature", best_feature)
    print ("AUC ", auc)
    
    selected.add(best_feature)
    available.remove(best_feature)

main()
