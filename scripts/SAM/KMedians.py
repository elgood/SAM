import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import f1_score, roc_auc_score, accuracy_score, precision_score, silhouette_score, \
    calinski_harabasz_score, davies_bouldin_score
from typing import List


class KMedians:
    def __init__(self, k: int = 2, max_iter: int = 100):
        self.k = k
        self.max_iter = max_iter

    def fit(self, X: np.ndarray) -> List[int]:
        centroids = self._initialize_centroids(X)

        for i in range(self.max_iter):
            clusters = self._create_clusters(X, centroids)
            new_centroids = self._update_centroids(X, clusters)
            if np.array_equal(new_centroids, centroids):
                break
            centroids = new_centroids

        self.centroids = centroids
        self.clusters = self._create_clusters(X, centroids)
        return self.clusters

    def _initialize_centroids(self, X: np.ndarray) -> np.ndarray:
        n_samples, n_features = X.shape
        centroids = np.zeros((self.k, n_features))
        for i in range(self.k):
            centroid = X[np.random.choice(range(n_samples))]
            centroids[i] = centroid
        return centroids

    def _create_clusters(self, X: np.ndarray, centroids: np.ndarray) -> List[int]:
        clusters = [[] for _ in range(self.k)]
        for idx, sample in enumerate(X):
            distances = [np.linalg.norm(sample - centroid) for centroid in centroids]
            cluster_idx = np.argmin(distances)
            clusters[cluster_idx].append(idx)
        return clusters

    def _update_centroids(self, X: np.ndarray, clusters: List[int]) -> np.ndarray:
        n_features = X.shape[1]
        centroids = np.zeros((self.k, n_features))
        for cluster_idx, cluster in enumerate(clusters):
            if len(cluster) == 0:
                # if a cluster is empty, leave the centroid as is
                centroids[cluster_idx] = self.centroids[cluster_idx]
            else:
                cluster_median = np.median(X[cluster], axis=0)
                centroids[cluster_idx] = cluster_median
        return centroids


def load_features(file_path: str) -> np.ndarray:
    # with open(file_path, 'r') as f:
    #     lines = f.readlines()
    # features = []
    # for line in lines:
    #     feature = [float(f) for f in line.strip().split()]
    #     features.append(feature)
    # return np.array(features)

    with open(file_path, 'r') as f:
        features = np.loadtxt(f, delimiter=',')
    return np.array(features)


def evaluate_clustering(X: np.ndarray, clusters: List[int], labels: np.ndarray):
    f1 = f1_score(labels, clusters, average='weighted')
    auc_roc = roc_auc_score(labels, clusters, average='weighted', multi_class='ovr')
    accuracy = accuracy_score(labels, clusters)
    precision = precision_score(labels, clusters, average='weighted')
    silhouette = silhouette_score(X, clusters, metric='euclidean')
    ch_score = calinski_harabasz_score(X, clusters)
    db_score = davies_bouldin_score(X, clusters)
    return {'F1 Score': f1, 'AUC ROC Score': auc_roc, 'Accuracy': accuracy, 'Precision': precision,
            'Silhouette Score': silhouette, 'Calinski-Harabasz Score': ch_score, 'Davies-Bouldin Score': db_score}


def run_kmedians_on_files(files: List[str], k: int, max_iter: int) -> List[dict]:
    results = []
    for file_path in files:
        # load features and labels from file
        features = load_features(file_path)
        labels = features[:, -1]
        features = features[:, :-1]

        # run k-medians clustering
        km = KMedians(k=k, max_iter=max_iter)
        clusters = km.fit(features)

        # evaluate clustering performance
        metrics = evaluate_clustering(features, clusters, labels)
        # metrics={}

        # evaluate additional clustering metrics
        silhouette = silhouette_score(features, clusters)
        chs = calinski_harabasz_score(features, clusters)
        db = davies_bouldin_score(features, clusters)

        metrics.update({'Silhouette Score': silhouette, 'Calinski-Harabasz Score': chs, 'Davies-Bouldin Score': db})

        results.append(metrics)
    return results

if __name__ == "__main__":

    filename = 'features.txt'
    file_paths = []
    for i in [5, 7]:
        # Replace path of files with where the data is located
        file_paths.append('C:/Users/shq_s/OneDrive - UCB-O365/Documents/data/' + str(i) + '/' + filename)

    k_values = [2, 3, 4]
    max_iter = 100

    for k in k_values:
        results = run_kmedians_on_files(file_paths, k, max_iter)
        print(results)


