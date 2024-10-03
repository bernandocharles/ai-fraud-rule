#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import math
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from math import log
from scipy.stats import chi2
from sklearn.ensemble import IsolationForest
from sklearn.datasets import make_classification
from sklearn.ensemble import RandomForestClassifier
from matplotlib import pyplot

import matplotlib.colors as mcolors
from matplotlib.colors import ListedColormap
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
from sklearn.covariance import MinCovDet

class data_processor:
    def __init__(self):
        self.objects = {}

    def process_train(self, feature_data: pd.DataFrame, model: str) -> pd.DataFrame:
        if not isinstance(model, str):
            raise TypeError("model input must be str")

        if model not in ["mcd", "iso", "isomcd"]:
            raise ValueError("model supported: mcd, iso, isomcd")

        if model == "mcd":
            self.objects["mcd"] = {}
            training_data = self.fit_mcd_preprocessing(
                feature_data=feature_data, model=model
            )

        if model == "iso":
            self.objects["iso"] = {}
            training_data = self.fit_iso_preprocessing(
                feature_data=feature_data, model=model
            )

        if model == "isomcd":
            self.objects["isomcd"] = {}
            training_data = self.fit_isomcd_preprocessing(
                feature_data=feature_data, model=model
            )

        return None

    def process_inference(self, feature_data: pd.DataFrame, model: str) -> pd.DataFrame:
        if not isinstance(model, str):
            raise TypeError("model input must be str")

        if model not in ["mcd", "iso", "isomcd"]:
            raise ValueError("model supported: mcd, iso, isomcd")

        if model == "mcd":
            if self.objects.get("mcd") is None:
                raise KeyError("mcd processor has not been fitted")
            inference_data = self.fit_mcd_preprocessing(
                feature_data=feature_data, model=model
            )

        if model == "iso":
            if self.objects.get("iso") is None:
                raise KeyError("iso processor has not been fitted")
            inference_data = self.fit_iso_preprocessing(
                feature_data=feature_data, model=model
            )

        if model == "isomcd":
            if self.objects.get("isomcd") is None:
                raise KeyError("isomcd processor has not been fitted")
            inference_data = self.fit_isomcd_preprocessing(
                feature_data=feature_data, model=model
            )

        return inference_data

    def fit_mcd_preprocessing(
        self, feature_data: pd.DataFrame, model: str
    ) -> pd.DataFrame:

        df = feature_data[feature_data["transaction_date"].notna()]

        df = df.set_index(["account_id", "transaction_date"])

        df = df[~df.index.duplicated(keep="first")]

        # Select relevant columns
        df = df[
            [
                "correction"
            ]
        ]
        #print(len(df.columns))
        #df["trx_5k"] = df["trx_5k"].fillna(0)
        #df["trx_10k"] = df["trx_10k"].fillna(0)
        #df["time_0_5"] = df["time_0_5"].fillna(0)
        #df["time_0_3"] = df["time_0_3"].fillna(0)
        #df["time_21_0"] = df["time_21_0"].fillna(0)
        #df["trxdest1m"] = df["trxdest1m"].fillna(0)
        #df["trxdest10m"] = df["trxdest10m"].fillna(0)
        #df["trxdest1h"] = df["trxdest1h"].fillna(0)
        #df["trxdest1d"] = df["trxdest1d"].fillna(0)
        #df["dest1m"] = df["dest1m"].fillna(0)
        #df["dest30m"] = df["dest30m"].fillna(0)


        #print(len(df.columns))

        old_columns = df.columns

        return df

    def fit_iso_preprocessing(
        self, feature_data: pd.DataFrame, model: str
    ) -> pd.DataFrame:
        df = feature_data[feature_data["transaction_date"].notna()]

        df = df.set_index(["account_id", "transaction_date"])

        df = df[~df.index.duplicated(keep="first")]

        # Select relevant columns
        df = df[
            [
                "correction"
            ]
        ]
        #print(len(df.columns))
        #df["trx_5k"] = df["trx_5k"].fillna(0)
        #df["trx_10k"] = df["trx_10k"].fillna(0)
        #df["time_0_5"] = df["time_0_5"].fillna(0)
        #df["time_0_3"] = df["time_0_3"].fillna(0)
        #df["time_21_0"] = df["time_21_0"].fillna(0)
        #df["trxdest1m"] = df["trxdest1m"].fillna(0)
        #df["trxdest10m"] = df["trxdest10m"].fillna(0)
        #df["trxdest1h"] = df["trxdest1h"].fillna(0)
        #df["trxdest1d"] = df["trxdest1d"].fillna(0)
        #df["dest1m"] = df["dest1m"].fillna(0)
        #df["dest30m"] = df["dest30m"].fillna(0)

        #print(len(df.columns))

        # df["inout5m"] = df["inout5m"].apply(self.func_inout)
        # df["inout30m"] = df["inout30m"].apply(self.func_inout)
        # df["inout1h"] = df["inout1h"].apply(self.func_inout)

        old_columns = df.columns

        #print(len(df[old_columns].columns))

        #df = self.scale_data(df[old_columns], model=model)

        return df

    def fit_isomcd_preprocessing(
        self, feature_data: pd.DataFrame, model: str
    ) -> pd.DataFrame:

        df = feature_data[feature_data["transaction_date"].notna()]

        df = df.set_index(["account_id", "transaction_date"])

        df = df[~df.index.duplicated(keep="first")]

        # Select relevant columns
        df = df[
            [
                "correction"
            ]
        ]
        #print(len(df.columns))
        #df["trx_5k"] = df["trx_5k"].fillna(0)
        #df["trx_10k"] = df["trx_10k"].fillna(0)
        #df["time_0_5"] = df["time_0_5"].fillna(0)
        #df["time_0_3"] = df["time_0_3"].fillna(0)
        #df["time_21_0"] = df["time_21_0"].fillna(0)
        #df["trxdest1m"] = df["trxdest1m"].fillna(0)
        #df["trxdest10m"] = df["trxdest10m"].fillna(0)
        #df["trxdest1h"] = df["trxdest1h"].fillna(0)
        #df["trxdest1d"] = df["trxdest1d"].fillna(0)
        #df["dest1m"] = df["dest1m"].fillna(0)
        #df["dest30m"] = df["dest30m"].fillna(0)

        #print(len(df.columns))

        old_columns = df.columns

        return df


    def func_inout(self, feature_data: pd.DataFrame, column: str) -> pd.DataFrame:
        for value in feature_data[column].values:
            if value != 1:
                value = abs(1 / log(value))
            else:
                value = 300000

        return feature_data[column]

    def scale_data(
        self,
        feature_data: pd.DataFrame,
        model: str,
        scaler_name: str = "standard_scaler",
    ) -> pd.DataFrame:
        if self.objects[model].get(scaler_name) is None:
            fitted = False
        else:
            fitted = True

        if fitted:
            scaler = self.objects[model][scaler_name]
            feature_data = pd.DataFrame(
                scaler.transform(feature_data),
                columns=scaler.get_feature_names_out(feature_data.columns),
                index=feature_data.index,
            )

        else:
            scaler = StandardScaler()
            scale_columns = feature_data.select_dtypes(
                ["int", "uint8", "float"]
            ).columns.to_list()
            feature_data = scaler.fit_transform(feature_data)
            self.objects[model][scaler_name] = scaler

        return feature_data


    #Remove features with high correlation
    def correlation(self, dataset, cor):
        df = dataset.copy()
        col_corr = set()  # For storing unique value
        corr_matrix = dataset.corr()
        for i in range(len(corr_matrix.columns)):
            for j in range(i):
                if abs(corr_matrix.iloc[i, j]) > cor: # absolute values to handle positive and negative correlations
                    colname = corr_matrix.columns[i]  
                    col_corr.add(colname)
        df.drop(col_corr,axis = 1,inplace = True)
        return df


#    def pca_data(
#        self,
#        feature_data: pd.DataFrame,
#        reduced_columns: list[str],
#        model: str,
#        components: int,
#        pca_name_prefix: str = '',
#    ) -> pd.DataFrame:

#        if self.objects[model].get(f"{pca_name_prefix}_pca") is None:
#            fitted = False
#        else:
#            fitted = True

#        if fitted:
#            pca = self.objects[model][f"{pca_name_prefix}_pca"]

#            tmp_data1 = feature_data.drop(pca[1], axis=1)

#            tmp_data2 = pd.DataFrame(
#                pca[0].transform(feature_data[pca[1]]),
#                columns = pca[2],
#                index=feature_data.index
#            )

#            feature_data = tmp_data1.join(tmp_data2)

#        else:
#            pca = PCA(n_components=components)
#            tmp_data1 = feature_data.drop(reduced_columns, axis=1)

#            tmp_data2 = pd.DataFrame(
#                pca.fit_transform(feature_data[reduced_columns]),
#                columns = [f"{pca_name_prefix}_{name}" for name in pca.get_feature_names_out()],
#                index=feature_data.index
#            )

#            feature_data = tmp_data1.join(tmp_data2)

#            self.objects[model][f"{pca_name_prefix}_pca"] \
#            = (pca,
#               reduced_columns,
#               [f"{pca_name_prefix}_{name}" for name in pca.get_feature_names_out()])

#        return feature_data


class MCD:
    def train(self, data, support_fraction=0.99):
        mcd = MinCovDet(support_fraction=support_fraction, random_state=42)
        mcd.fit(data)

        self.mean_ = mcd.location_
        self.covariance_ = mcd.covariance_
        self.covariance_inverse_ = np.linalg.pinv(mcd.covariance_)
        self.dist_ = mcd.dist_

        return None

    def inference(self, data, threshold_method, percentgenuine):
        inference_data = data
        inference = self.mcd_infer(
            inference_data,
            self.mean_,
            self.covariance_inverse_,
            threshold_method,
            percentgenuine
        )

        inference_data["anomaly_score"] = inference[0]
        inference_data["outlier_status"] = inference[1]

        return inference_data

    def mcd_infer(self, record, mcd_mean, mcd_cov_inv, threshold_method, genuine):
        centered = record - mcd_mean

        mahalanobis_dist = (np.dot(centered, mcd_cov_inv) * centered).sum(axis=1)

        if threshold_method == "chi2":
            rv = chi2(df=record.shape[1])
            threshold = rv.ppf(genuine)
        elif threshold_method == "quantile":
            threshold = np.quantile(self.dist_, genuine)

        #outlier_status = mahalanobis_dist > threshold

        #print(mahalanobis_dist)
        #print(threshold)
        #print(outlier_status)

        outlier_status = mahalanobis_dist.map(lambda x: 1 if (x < threshold) else -1)
        print(outlier_status)
        #if mahalanobis_dist < threshold:
        #    outlier_status = 1
        #else:
        #    outlier_status = -1

        self.threshold = threshold

        return mahalanobis_dist, outlier_status


# Class for ISO algorithm

class ISO:
    def train(self, data, contamination):
        iso = IsolationForest(
            n_estimators=100,
            max_samples="auto",
            contamination=contamination,
            max_features=1.0,
            bootstrap=False,
            n_jobs=-1,
            random_state=42,
            verbose=0,
        )
        iso.fit(data)

        self.decision_function = iso.decision_function
        self.predict = iso.predict

        return None

    def inference(self, data, threshold_method, percentgenuine):
        inference_data = data
        inference = self.iso_infer(inference_data, threshold_method, percentgenuine)

        inference_data["anomaly_score"] = inference[0]
        inference_data["outlier_status"] = inference[1]

        return inference_data

    def iso_infer(self, record, threshold_method, genuine):
        anomaly_score = self.decision_function(record)

        if threshold_method == "quantile":
            threshold = np.quantile(anomaly_score, genuine)

        #outlier_status = anomaly_score < 0
        
        outlier_status = self.predict(record)

        self.threshold = threshold

        return anomaly_score, outlier_status

    def outlier_data(data):
        outlier = data.loc[data["outlier_status"] == -1]

        outlier_index = list(outlier.index)

        # print(data["outlier_status"].value_counts())

        return outlier

# Class for ISOMCD algorithm

class ISOMCD:
    def train(self, data, contamination, support_fraction=0.99):
        # MCD part
        mcd = MinCovDet(support_fraction=support_fraction, random_state=42)
        mcd.fit(data)

        self.mean_ = mcd.location_
        self.covariance_ = mcd.covariance_
        self.covariance_inverse_ = np.linalg.pinv(mcd.covariance_)
        self.dist_ = mcd.dist_

        # ISO part
        iso = IsolationForest(
            n_estimators=100,
            max_samples="auto",
            contamination=contamination,
            max_features=1.0,
            bootstrap=False,
            n_jobs=-1,
            random_state=42,
            verbose=0,
        )
        iso.fit(data)

        self.decision_function = iso.decision_function
        self.predict = iso.predict

        return None

    def inference(self, data, threshold_method, percentgenuine):
        inference_data = data
        
        # MCD and ISO in one inference
        inference = self.isomcd_infer(
            inference_data,
            self.mean_,
            self.covariance_inverse_,
            threshold_method,
            percentgenuine
        )

        inference_data["anomaly_score"] = inference[0]
        inference_data["outlier_status"] = inference[1]

        return inference_data

    def isomcd_infer(self, record, mcd_mean, mcd_cov_inv, threshold_method, genuine):
        # MCD part
        centered = record - mcd_mean
        mahalanobis_dist = (np.dot(centered, mcd_cov_inv) * centered).sum(axis=1)
        print("---Mahalanobis distance---")
        print(mahalanobis_dist)

        # ISO part
        anomaly_score = self.decision_function(record)
        print("---Anomaly score---")
        print(anomaly_score)

        # MCD and ISO thresholds
        if threshold_method == "quantile":
            thresholdmcd = np.quantile(self.dist_, genuine)
            thresholdiso = np.quantile(anomaly_score, genuine)

        self.thresholdiso = thresholdiso
        self.thresholdmcd = thresholdmcd

        # MCD Outlier status
        mcd_outlier_status = mahalanobis_dist.map(lambda x: 1 if (x < thresholdmcd) else -1)
        print("---mcd outlier status---")
        print(mcd_outlier_status)
        print(type(mcd_outlier_status))

        # ISO Outlier status
        iso_outlier_status = self.predict(record)
        #record['anomali'] = self.predict(record)
        #record['skoranomali'] = self.decision_function(record)
        print("---iso outlier status---")
        print(iso_outlier_status)
        print(type(iso_outlier_status))
        #print(record.head(5))
        #print(type(record))

        iso_outlier_status_ser = pd.Series(iso_outlier_status, index=record.index)
        print("---Series from Numpy---")
        print(iso_outlier_status_ser.to_string())
        print(type(iso_outlier_status_ser))

        #ser1 = record.iloc[:,10]
        #print("---Series from Pandas dataframe---")
        #print(ser1.to_string())
        #print(type(ser1))

        # ISO and MCD Outlier status
        isomcddf = pd.concat({'mcd':mcd_outlier_status, 'iso':iso_outlier_status_ser}, axis=1)
        print("---isomcd dataframe---")
        print(isomcddf.to_string())
        
        isomcddf['isomcd_status'] = np.where((isomcddf['mcd'] == -1) & (isomcddf['iso'] == -1), -1, 1)
        print("---isomcd dataframe with status---")
        print(isomcddf.to_string())

        outlier_status = isomcddf.iloc[:,2]
        print("---outlier status---")
        print(outlier_status)
        print(type(outlier_status))

        score = anomaly_score*mahalanobis_dist
        
        return score, outlier_status


class models:
    def __init__(self):
        self.models = {}

    def model_train(self, training_data: pd.DataFrame, model: str, contamination) -> pd.DataFrame:
        if not isinstance(model, str):
            raise TypeError("model input must be str")

        if model not in ["mcd", "iso", "isomcd"]:
            raise ValueError("model supported: mcd, iso, isomcd")

        if model == "mcd":
            mcd = MCD()
            mcd.train(training_data, support_fraction=0.99)
            self.models["mcd"] = mcd
        elif model == "iso":
            iso = ISO()
            iso.train(training_data, contamination)
            self.models["iso"] = iso
        elif model == "isomcd":
            isomcd = ISOMCD()
            isomcd.train(training_data, contamination, support_fraction=0.99)
            self.models["isomcd"] = isomcd

        return None

    def model_inference(self, inference_data: pd.DataFrame, model: str, genuine) -> pd.DataFrame:
        if model == "mcd":
            if self.models.get("mcd") is None:
                raise KeyError("mcd model has not been fitted")
            else:
                mod = self.models.get("mcd")
                result_df = mod.inference(inference_data, "quantile", genuine)
        elif model == "iso":
            if self.models.get("iso") is None:
                raise KeyError("iso model has not been fitted")
            else:
                mod = self.models.get("iso")
                result_df = mod.inference(inference_data, "quantile", genuine)
        elif model == "isomcd":
            if self.models.get("isomcd") is None:
                raise KeyError("isomcd model has not been fitted")
            else:
                mod = self.models.get("isomcd")
                result_df = mod.inference(inference_data, "quantile", genuine)

        return result_df

# Random Forest for feature importance
class RF:
    def train(self, data):
        X = data.drop("anomaly_score", axis=1).drop("outlier_status", axis=1)
        y = data["outlier_status"]

        rfmodel = RandomForestClassifier()

        rfmodel.fit(X, y)

        self.importance = rfmodel.feature_importances_

        return None


    def feature_importance(self, data):
        X = data.drop("anomaly_score", axis=1).drop("outlier_status", axis=1)

        for i, v in enumerate(self.importance):
            print("Feature: %0d, Score: %.5f" % (i, v))

        feat_importance = pd.Series(self.importance, index=X.columns)
        fig = feat_importance.plot(kind="bar", figsize=(20, 25), fontsize=26, x="Features", y="Importance", title="Feature Importance Plot").get_figure()
        fig.savefig("feature_importance.png")

        return feat_importance

    def top_n_feature(self, data, n):
        if not isinstance(n, int):
            raise TypeError("n is the top-n features, which is an integer")

        if n not in range(1, 25):
            raise ValueError("n has to be in a range of 1 to 25")

        new_columns = self.feature_importance(data).nlargest(n).index

        return new_columns



class rfmodels:
    def __init__(self):
        self.rfmodels = {}

    def rfmodel_train(self, training_data: pd.DataFrame, model: str) -> pd.DataFrame:
        if not isinstance(model, str):
            raise TypeError("model input must be str")

        if model not in ["rf"]:
            raise ValueError("model supported: rf")

        if model == "rf":
            rf = RF()
            rf.train(training_data)
            self.rfmodels["rf"] = rf

        return None

    def rfmodel_importance(
        self, inference_data: pd.DataFrame, model: str
    ) -> pd.DataFrame:
        if model == "rf":
            if self.rfmodels.get("rf") is None:
                raise KeyError("RF model has not been fitted to see feature importance")
            else:
                mod = self.rfmodels.get("rf")
                result = mod.feature_importance(inference_data)

        return result


# Do clustering on the anomalies

from sklearn.cluster import KMeans
from collections import Counter
from kneed import KneeLocator
from sklearn.metrics import silhouette_score
import math
from math import floor, ceil


class clustering:
    def numbercluster(self, data):
        inertias = []

        if data.shape[0] <= 200:
            k = 1
            print('Number of clusters: ', k)
        else:
            maxclust = min(floor(data.shape[0]/100), 20)

            for i in range(1,maxclust):
                kmeans = KMeans(n_clusters=i)
                kmeans.fit(data)
                inertias.append(kmeans.inertia_)

            number_clusters = range(1,maxclust)    

            plt.plot(number_clusters,inertias)
            plt.title('The Elbow')
            plt.xlabel('Number of clusters')
            plt.ylabel('Inertia')    

            # Obtain the optimum n value
            kneedle = KneeLocator(x=range(1, maxclust), y=inertias, curve='convex', direction='decreasing', S=2)
            knee_point = kneedle.knee 
            elbow_point = kneedle.elbow
            k = elbow_point
            print('Knee: ', knee_point) 
            print('Elbow: ', elbow_point)
            print('Number of clusters: ', k)

        return k

    def train(self, data, n, column):
        kmean = KMeans(n_clusters=n)
        kmean.fit_predict(data[column])
        data['label']=kmean.labels_
        Counter(kmean.labels_)
    
        if n > 1:
            silhouette_scores = silhouette_score(data[column], kmean.labels_)
            print('Silhouette score: ', silhouette_scores)
        else:
            print('Silhouette score is not defined for 1 cluster')

        return data
    
    #To find the index of event in each cluster
    def eventindexincluster(self, data, j):
        test = data.reset_index(drop=True)
        clust_index = []
        for i in range(max(test['label'])+1):
            clustindex = list(test.loc[test['label']==i].index)
            clust_index.append(clustindex)
        
        return clust_index[j]


# Decision Tree

class dtmodels:
    def __init__(self):
        self.dtmodels = {}

    def dtmodel_train(
        self, training_data: pd.DataFrame, test_size: float
    ) -> pd.DataFrame:
        if not isinstance(test_size, float):
            raise TypeError("test size must be float or double")

        if test_size not in range[0, 1]:
            raise ValueError("test size value is between 0.0 to 1.0")

        if test_size in range[0, 1]:
            dt = DT()
            training_data = self.preprocessing(training_data, test_size)
            dt.train(training_data)
            self.dtmodels["dt"] = dt

        return None

    def dtmodel_inference(
        self, inference_data: pd.DataFrame, model: str
    ) -> pd.DataFrame:
        if model == "dt":
            if self.dtmodels.get("dt") is None:
                raise KeyError("dt model has not been fitted")
            else:
                mod = self.dtmodels.get("dt")
                result_df = mod.inference(inference_data)

        return result_df



# Create new rule using Decision Tree
from imblearn.over_sampling import SMOTE, ADASYN, RandomOverSampler
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.tree import export_text
from sklearn import metrics
from sklearn.metrics import f1_score, accuracy_score, precision_score, recall_score
from sklearn.model_selection import cross_val_score
import numpy as np
import matplotlib.pyplot as plt
import re
from sklearn.tree import _tree
import math
from math import fmod, floor, exp
import json


class DT:
    def preprocessing(self, data, column, test_size=0.3):
        X = data.drop("outlier_status", axis=1).drop("anomaly_score", axis=1).to_numpy()
        y = data[["outlier_status"]].to_numpy()
        print(X)
        print(y)
        X_resampled, y_resampled = SMOTE(k_neighbors=3).fit_resample(X, y)

        Xrs = pd.DataFrame(X_resampled, columns=column)
        yrs = pd.DataFrame(y_resampled, columns=["outlier_status"])

        # split into train test sets
        X_train, X_test, y_train, y_test = train_test_split(
            Xrs, yrs, test_size=test_size, random_state=1
        )

        return X_train, X_test, y_train, y_test

    def train(self, xtrain, ytrain, depth=4, split=1000, leaf=500):
        decision_tree = DecisionTreeClassifier(
            random_state=0,
            max_depth=depth,
            min_samples_split=split,
            min_samples_leaf=leaf,
        )

        # dec_tree = decision_tree.fit(xtrain, ytrain)

        self.predict = decision_tree.predict
        self.dec_tree = decision_tree.fit(xtrain, ytrain)

        return None

    def inference(self, xtest, ytest, new_columns):
        ypred = self.predict(xtest)

        # accuracy_train = self.dec_tree.score(X_train, y_train)
        accuracy_test = self.dec_tree.score(xtest, ytest)

        r = export_text(self.dec_tree, feature_names=new_columns.tolist())

        print(r)
        print("F1 score: {:.2f}".format(f1_score(ytest, ypred)))
        print("Accuracy score:", accuracy_score(ytest, ypred))
        print("Precision score:", precision_score(ytest, ypred))
        print("Recall score:", recall_score(ytest, ypred))

        # dectree = std_scaler.inverse_transform(dec_tree)

        # inference_data["anomaly_score"] = inference[0]
        # inference_data["outlier_status"] = inference[1]

        return r

    # function for fitting trees of various depths on the training data using cross-validation
    def run_cross_validation_on_trees(self, Xtrain, ytrain, Xtest, ytest, tree_depths, min_split, min_leaf, cvfold, metric):
        cv_scores_list = []
        cv_scores_std = []
        cv_scores_mean = []
        accuracy_scores = []
    
        for depth in tree_depths:
            for split in min_split:
                for leaf in min_leaf:
                    tree_model = DecisionTreeClassifier(max_depth=depth, min_samples_split=split, min_samples_leaf=leaf)
                    cv_scores = cross_val_score(tree_model, Xtrain, y=ytrain, cv=cvfold, scoring=metric)
                    cv_scores_list.append(cv_scores)
                    cv_scores_mean.append(cv_scores.mean())
                    cv_scores_std.append(cv_scores.std())
                    accuracy_scores.append(tree_model.fit(Xtrain, ytrain).score(Xtest, ytest))
        cv_scores_mean = np.array(cv_scores_mean)
        cv_scores_std = np.array(cv_scores_std)
        accuracy_scores = np.array(accuracy_scores)

        idx_max = cv_scores_mean.argmax()
        print(idx_max)
        best_tree_depth = tree_depths[floor(idx_max/(len(min_split)*len(min_leaf)))]
        best_min_split = min_split[floor(idx_max/len(min_leaf)) % len(min_split)]
        best_min_leaf = min_leaf[idx_max % len(min_leaf)]
        
        return best_tree_depth, best_min_split, best_min_leaf
        #return cv_scores_mean, cv_scores_std, accuracy_scores


    def Filter(self, string, substr):
        return [str for str in string if any(sub in str for sub in substr)]

    # Rules representation in JSON

    def get_rules(self, tree, feature_names, class_names, type, source, unit, value, filters, scalers):
        tree_ = tree.tree_
        feature_name = [
            feature_names[i] if i != _tree.TREE_UNDEFINED else "undefined!"
            for i in tree_.feature
        ]

        paths = []
        path = []

    
        def recurse(node, path, paths):
        
            if tree_.feature[node] != _tree.TREE_UNDEFINED:
                name = feature_name[node]
                featuren = tree_.feature[node]
            
                threshold = tree_.threshold[node]
            
                # create empty table with 15 fields
                print(threshold.size)
                trainPredict_dataset_like = np.zeros(shape=(threshold.size, len(feature_names)) )
                # put the predicted values in the right field
                print(featuren)
                print(name)
                print(trainPredict_dataset_like)
                print(threshold)
                trainPredict_dataset_like[0, featuren] = threshold
                print(trainPredict_dataset_like)
                # inverse transform and then select the right field
                trainPredict = scalers.inverse_transform(trainPredict_dataset_like)[0, featuren]
                print(trainPredict)
            
                thres = trainPredict
                print(thres)
            
                if name == "inout5m" or name == "inout30m" or name == "inout1h":
                    if thres == 300000:
                        thresholdn = 1
                    else:
                        try:
                            thresholdn = exp(1/thres)
                        except OverflowError:
                            thresholdn = 300000
                else:
                    thresholdn = thres
            
                print(thresholdn)
            
            
                p1, p2 = list(path), list(path)
                p1 += [f'"{source[name]}", "type": "{type[name]}", "params_timeseries": {{"unit": "{unit[name]}", "value": {value[name]}}}, "params_check": {{"value": {np.round(thresholdn, 3)}, "condition": "<="}}, "filter": {filters[name]}' + '}}']
                recurse(tree_.children_left[node], p1, paths)
                p2 += [f'"{source[name]}", "type": "{type[name]}", "params_timeseries": {{"unit": "{unit[name]}", "value": {value[name]}}}, "params_check": {{"value": {np.round(thresholdn, 3)}, "condition": ">"}}, "filter": {filters[name]}' + '}}']
                recurse(tree_.children_right[node], p2, paths)
            else:
                path += [(tree_.value[node], tree_.n_node_samples[node])]
                paths += [path]
                        
        recurse(0, path, paths)

        
        # sort by samples count
        samples_count = [p[-1][1] for p in paths]
        ii = list(np.argsort(samples_count))
        paths = [paths[i] for i in reversed(ii)]
    
        rules = []
        for path in paths:
            rule = '{"feature": {"source_id": '
        
            for p in path[:-1]:
                if rule != '{"feature": {"source_id": ':
                    rule += ', {"feature": {"source_id": '
                rule += str(p)
            rule += " then "
            if class_names is None:
                rule += "response: "+str(np.round(path[-1][0][0][0],3))
            else:
                classes = path[-1][0][0]
                l = np.argmax(classes)
                rule += f"class: {class_names[l]}"
            #rule += f" | based on {path[-1][1]:,} samples"
            rules += [rule]
            substr = ['class: -1']
            rules = self.Filter(rules,substr)
        
        return rules


    def rule_json(self, ruless):
        substring = " then class: -1"
        rule_string = ""
        for r in ruless:
            print(r)
            output = r.replace(substring, "")
            print(output)
            output = "[" + output + "]"
            print(output)
            # json_object = json.loads(output)
            rule_string += output + ","
        print(rule_string)

        #string_rule = "[" + rule_string[:-1] + "]"
        #json_rule = json.loads(string_rule)
        #print(json_rule)
        return rule_string


    ## Decision Tree for all clusters
    def training(self, data, kk, column, tree_depths_range, min_split_range, min_leaf_range, target_names, type, source, unit, value, filters, scales):
        substring = " then class: -1"
        rulestring = ""
        for i in range(kk):
            print("---cluster " + str(i) + "---")
            X = data[(data['label']==i)|(data["outlier_status"]==1)].drop('outlier_status', axis=1).drop('anomaly_score', axis=1).drop('label', axis=1).to_numpy()
            print(X)
            y = data[['outlier_status']][(data['label']==i)|(data["outlier_status"]==1)].to_numpy()
            print(y)
            total_outlier = (y == -1).sum()
            print(total_outlier)

            if total_outlier <= 3:
                ros = RandomOverSampler(sampling_strategy='minority', random_state=42)
                X_resampled, y_resampled = ros.fit_resample(X, y)
            elif total_outlier > 3 and total_outlier < 10: 
                X_resampled, y_resampled = SMOTE(k_neighbors=2).fit_resample(X, y)
            else:
                X_resampled, y_resampled = SMOTE(k_neighbors=5).fit_resample(X, y)
            print(X_resampled)
            print(y_resampled)
            Xrs = pd.DataFrame(X_resampled, columns=column)
            yrs = pd.DataFrame(y_resampled, columns=['outlier_status'])
            print(Xrs)
            print(yrs)
            # split into train test sets
            X_train, X_test, y_train, y_test = train_test_split(Xrs, yrs, test_size=0.2, random_state=1)
            print(X_train)
            print(X_test)
            print(y_train)
            print(y_test)

            if kk == 1:
                best_depth = 1
                best_split = 3
                best_leaf = 3
            else:
                best_depth, best_split, best_leaf = self.run_cross_validation_on_trees(X_train, y_train, X_test, y_test, tree_depths_range, min_split_range, min_leaf_range, 5, 'f1')
                print(best_depth)
                print(best_split)
                print(best_leaf)
     
            decision_tree = DecisionTreeClassifier(max_depth=best_depth, min_samples_split=best_split, min_samples_leaf=best_leaf, random_state=0)
            dec_tree = decision_tree.fit(X_train, y_train)
            y_pred = dec_tree.predict(X_test)
            print(y_pred)

            r = export_text(dec_tree, feature_names=column.tolist())
    
            accuracy_train = dec_tree.score(X_train, y_train)
            accuracy_test = dec_tree.score(X_test, y_test)
    
            print(r)
            print("F1 score: {:.2f}".format(f1_score(y_test,y_pred)))
            print('Accuracy score:', accuracy_score(y_test,y_pred))
            print('Precision score:', precision_score(y_test,y_pred))
            print('Recall score:', recall_score(y_test,y_pred))
            print('Accuracy, Training Set: ', round(accuracy_train*100,5), '%')
            print('Accuracy, Test Set: ', round(accuracy_test*100,5), '%')
    
            rules = self.get_rules(dec_tree, column.tolist(), target_names, type, source, unit, value, filters, scales)
            print(rules)

            #output = rules.replace(substring, "")
            #print(output)
            #output = "[" + output + "]"
            #print(output)
        
            #rule_string += output + ","
            #print(rule_string)

            fraud_rule = self.rule_json(rules)

            rulestring = rulestring + fraud_rule

        print("rule string: START")
        print(rulestring)
        print("rule string: END")

        extract = [x for x in re.split(r'\]\,\[', rulestring[:-1]) if x.strip()]
        print(extract)
        print(len(extract))

        if len(extract) == 1:
            string_rule = rulestring[:-1]
            print("string rule")
            print(string_rule)

            json_rule = json.loads(string_rule)
            print("json rule")
            print(json_rule)
        else:
            string_rule = "[" + rulestring[:-1] + "]"
            print("string rule")
            print(string_rule)

            json_rule = json.loads(string_rule)
            print("json rule")
            print(json_rule)

        return json_rule


#    def rule_json(self, ruless):
#        substring = " then class: -1"
#        rule_string = ""
#        for r in ruless:
#            print(r)
#            output = r.replace(substring, "")
#            print(output)
#            output = "[" + output + "]"
#            print(output)
#            #json_object = json.loads(output)
#            rule_string += output + ","
#        print(rule_string)

#        string_rule = "[" + rule_string[:-1] + "]"
#        json_rule = json.loads(string_rule)
#        print(json_rule)
#        return json_rule