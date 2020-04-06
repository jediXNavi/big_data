# anomaly_detection.py
import pandas as pd
from pandas.core.common import flatten
import numpy as np
from sklearn.cluster import KMeans

class AnomalyDetection():

    def scaleNum(self, df, indices):

        #Scaling features to ensure consistent weight across all features
        features_df = pd.DataFrame(df['features'].to_list()).astype(float)
        feature_to_be_standardized = features_df[indices]

        #Calculating the mean and std of the feature to be scaled to perform feature scaling
        mean_value = list(feature_to_be_standardized.mean().values)
        std_value = list(np.sqrt(feature_to_be_standardized.var().values))

        for i,value in enumerate(indices):
            features_df[value] = features_df[value].apply(lambda x: (x - mean_value[i])/std_value[i])
        features_df['features'] = features_df.values.tolist()
        features_df['id'] = features_df.index
        standardized_features = features_df[['id','features']]

        return standardized_features

    def cat2Num(self, df, indices):

        features_df = pd.DataFrame(df['features'].to_list())
        cat_feature_df = features_df[indices]
        categorical_cols = len(cat_feature_df.columns)
        numerical_df = features_df.iloc[:,categorical_cols:]
        category_dict = {}

        #Using length of np.identity matrix to create one-hot vectors
        for i in range(categorical_cols):
            unique_elements = cat_feature_df[i].unique()
            for index, value in enumerate(unique_elements):
                category_dict[value] = np.identity(len(unique_elements))[index]

            cat_feature_df[i] = cat_feature_df[i].apply(lambda x:category_dict[x])

        #Concatenating to get a feature vector
        feature_vector = pd.concat([cat_feature_df,numerical_df],axis=1)
        feature_vector['features'] = feature_vector.values.tolist()
        feature_vector['features'] = feature_vector['features'].apply(lambda x: list(flatten(x)))
        feature_vector['id'] = feature_vector.index
        feature_vector = feature_vector[['id','features']]

        return feature_vector

    def detect(self, df, k, t):

        features = pd.DataFrame(df['features'].to_list())
        features = features.fillna(0)

        #Running K Means Clustering algorithm
        kmeans = KMeans(n_clusters=k,random_state=3)
        model = kmeans.fit(features)
        features['features'] = features.values.tolist()
        features['cluster_label'] = model.labels_

        cluster_counts = features['cluster_label'].value_counts()
        Nmax = cluster_counts.max()
        Nmin = cluster_counts.min()
        cluster_dict = cluster_counts.to_dict()

        features['score'] = features['cluster_label'].apply(lambda x: (Nmax - cluster_dict[x])/(Nmax-Nmin))
        features = features[features['score']>=t]
        features['id'] = features.index
        verified_networks = features[['id','features','score']]

        return verified_networks

if __name__ == "__main__":
    df = pd.read_csv('logs-features-sample.csv').set_index('id')
    df = df.rename(columns={"rawFeatures": "features"}, errors="ignore")

    df['features'] = df['features'].apply(lambda x:x.strip('][').split(', '))

    ad = AnomalyDetection()

    df1 = ad.cat2Num(df, [0,1])
    print(df1)
    df2 = ad.scaleNum(df1, [16])
    print(df2)

    df3 = ad.detect(df2, 8, 0.97)
    print(df3)