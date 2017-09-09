import numpy as np
import argparse
from sklearn.model_selection import cross_val_score
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier
from sklearn import svm
import sklearn

def main():

  parser = argparse.ArgumentParser()
  parser.add_argument('--inputfile', type=str,
                      required=True,
                      help="The file with the features and labels.")

  FLAGS = parser.parse_args()

  with open(FLAGS.inputfile, "r") as infile:
    data = np.loadtxt(infile, delimiter=",")

    y = data[:, 0]
    X = data[:, 1:] 
    
    y1 = y[0: len(y) / 2]
    y2 = y[len(y) / 2 :]
    print len(y1), len(y2)   

    X1 = X[0: len(y) / 2]
    X2 = X[len(y) / 2 :]
    print X1.shape, X2.shape
 
    clf = svm.SVC(class_weight="balanced")
    #clf.fit(X1, y1)
    clf.fit(X1, y1)

    print metrics.accuracy_score(y1, clf.predict(X1))
    print metrics.accuracy_score(y2, clf.predict(X2))
    #print clf.predict_proba(X1)
    #print metrics.roc_auc_score(y1, clf.predict_proba(X1)[:,1])
    #print metrics.roc_auc_score(y2, clf.predict_proba(X2)[:,1])

    print np.count_nonzero(y1), len(y1) 
    print np.count_nonzero(y2), len(y2) 

    clf.fit(X2, y2)

    print metrics.accuracy_score(y1, clf.predict(X1))
    print metrics.accuracy_score(y2, clf.predict(X2))
    #print clf.predict_proba(X1)
    #print metrics.roc_auc_score(y1, clf.predict_proba(X1)[:,1])
    #print metrics.roc_auc_score(y2, clf.predict_proba(X2)[:,1])

    print np.count_nonzero(y1), len(y1) 
    print np.count_nonzero(y2), len(y2) 


    #scores = cross_val_score(clf, X, y, cv=5, scoring='roc_auc')

    #print scores

main()
