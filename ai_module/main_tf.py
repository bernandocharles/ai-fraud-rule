#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import classes_tf
import utils
from classes_tf import data_processor, MCD, ISO, ISOMCD, models, RF, rfmodels, clustering, dtmodels, DT
from utils import target_names, value, unit, type, source, filters
import os
import sys
from sklearn.preprocessing import StandardScaler
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import json
from math import floor

#Parse command line arguments
parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-m", "--anomalymodel", default="iso", help="Machine learning model for anomaly detection. The options are iso, mcd, or isomcd")
parser.add_argument("-a", "--percentanomaly", default=10, type=float, help="Percent of anomaly events exist in the dataset")
parser.add_argument("-f", "--featureselection", action="store_true", help="Include feature selection steps")
parser.add_argument("-i", "--inputpath", default="./", help="Input path of file")
parser.add_argument("-o", "--outputpath", default="./", help="Output path of file")
args = vars(parser.parse_args())
print(args)

#JSON status
json_status = '{}'

# Set up parameters
featsel = args["featureselection"]
anomalymodel = args["anomalymodel"]
percentanomaly = args["percentanomaly"]
anomaly = percentanomaly/100
genuines = 1 - anomaly

input_path = args["inputpath"]
output_path = args["outputpath"]
# Directory of the input data
#dirs = "nobu_all_202305302143.csv"
dirs = f"{input_path}/transfer.csv"
print(f"file input from {dirs}")

print("---Import event data---")
df = pd.read_csv(dirs)
print(df.head(5))
print(df.shape)

#Procedure to check the dataset
rows_count = df.shape[0]

if rows_count == 0:
     json_rule = "[]"
     print(json_rule)
     fraud_rule = json.loads(json_rule)
     print(fraud_rule)
     json_status = '{"status": "Empty input data"}'
     print(json_status)
elif rows_count < 100 or anomaly*rows_count < 10:
    json_rule = "[]"
    print(json_rule)
    fraud_rule = json.loads(json_rule)
    print(fraud_rule)        
    json_status = '{"status": "Insufficient input data. Try to add more data and higher anomaly percentage"}'
    print(json_status)
else:
    print("---Data preprocessing---")
    dp = data_processor()
    dp.objects
    # Fit model preprocessing
    dp.process_train(df, model=anomalymodel)
    dp.objects
    print(dp.objects)


    # Old df with old columns
    print("---Dataframe after data preprocessing---")
    dffinal = dp.process_inference(df, model=anomalymodel)
    old = dffinal.columns
    print("--Original dataframe--")
    print(dffinal.head(5))
    print(dffinal.shape)
    print(old)


    print("---Dataframe after data scaling---")
    scaler = StandardScaler()
    scale_columns = dffinal.select_dtypes(["int", "uint8", "float"]).columns.to_list()
    scalers = scaler.fit_transform(dffinal[old].to_numpy())
    df_scaled = pd.DataFrame(scalers, index=dffinal.index, columns=old)
    old_columns = df_scaled.columns
    print(df_scaled.head(5))
    print(df_scaled.shape)
    print(old_columns)

    # New df with new columns due to correlation
    print("---Dataframe after feature selection---")
    if featsel == False:
        df_final = df_scaled
    else:
        df_final = dp.correlation(df_scaled, 0.8)
    new_columns = df_final.columns
    print(df_final.head(5))
    print(df_final.shape)
    print(new_columns)

    ## Model ISO or MCD
    print("---Anomaly detection model---")
    mod = models()
    mod.models

    mod.model_train(df_final, model=anomalymodel, contamination=anomaly)
    mod.models

    # Obtain the data frame after addition of anomaly score and status
    print("---Output after the anomaly detection model---")
    dfs = mod.model_inference(df_final, model=anomalymodel, genuine=genuines)
    out=dfs.loc[dfs["outlier_status"]==-1]
    print(dfs.head(5))
    print(dfs.shape)
    print(out.head(5))
    print(out.shape)

    if out.shape[0] < 10:
        json_rule = "[]"
        print(json_rule)
        fraud_rule = json.loads(json_rule)
        print(fraud_rule)
        json_status = '{"status": "No anomalies detected for transfer"}'
        print(json_status)

    elif out.shape[0] >= 10:
        ## Model RF for feature importance
        print("---Feature importance process---")
        rfmod = rfmodels()
        rfmod.rfmodels

        rfmod.rfmodel_train(dfs, model="rf")
        rfmod.rfmodels

        featimportance = rfmod.rfmodel_importance(dfs, model="rf")

        # Inverse transform the dfs into the original df_ori, then transform only the selected new_columns in df_ori
        print("---New dataframe with selected columns for clustering process---")
        ori = scaler.inverse_transform(df_scaled[old_columns])
        df_ori = pd.DataFrame(ori, columns=old_columns, index=dffinal.index)
        #print(ori)
        print(df_ori.head(5))
        print(df_ori.shape)
        print(df_ori.columns)

        scale = StandardScaler()
        scaled = scale.fit_transform(df_ori[new_columns].to_numpy())
        dfscaled = pd.DataFrame(scaled, index=dffinal.index, columns=new_columns)
        print(dfscaled.head(5))
        print(dfscaled.shape)
        print(dfscaled.columns)

        print("---Final dataframe for input of clustering---")
        # The new df with new columns, scores and anomalies 
        dfnew = pd.concat([dfscaled, dfs['anomaly_score'], dfs['outlier_status']], axis=1)
        outlier=dfnew.loc[dfnew["outlier_status"]==-1]
        outlier_index=list(outlier.index)
        print(dfnew.head(5))
        print(dfnew.shape)
        print(dfnew.columns)
        print(outlier.head(5))
        print(outlier.shape)
        print(outlier.columns)

        ## K-Means for clustering the outliers
        print("---K-Means clustering---")
        clust = clustering()
        k = clust.numbercluster(outlier)

        if k == None:
            k = floor(outlier.shape[0]/100)
        else:
            k = k
        
        outclust = clust.train(outlier, k, new_columns)
        print(k)
        print(outclust.head(5))
        print(outclust.shape)
        print(outclust.columns)

        ## Data prep for DT
        print("---Dataframe for input of decision tree---")
        dfo = pd.concat([dfnew, outclust['label']], axis=1)
        print(dfo.head(5))
        print(dfo.shape)
        print(dfo.columns)

        ## Decision Tree
        print("---Decision Tree process---")
        sm_tree_depths = range(1,3)
        sm_min_split = [100, 500, 1000]
        sm_min_leaf = [100, 500, 1000]

        dtinstance = DT()
        #Xtrain, Xtest, ytrain, ytest = dtmodel.preprocessing(dfo, new_columns, test_size=0.3)
        fraud_rule = dtinstance.training(dfo, k, new_columns, sm_tree_depths, sm_min_split, sm_min_leaf, utils.target_names, utils.type, utils.source, utils.unit, utils.value, utils.filters, scale)

    print("---Fraud Rules---")
    print(f"===\n {fraud_rule}")

with open(f'{output_path}/transfer.json','w') as f:
    rule_json = json.dumps(fraud_rule)
    print(rule_json, file=f)

with open(f'{output_path}/transfer_status.json','w') as f:
    status_json = json.dumps(json.loads(json_status))
    print(status_json, file=f)
