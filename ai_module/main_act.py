#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import classes_act
import utils_act
from classes_act import data_processor, MCD, ISO, ISOMCD, models, RF, rfmodels, clustering, dtmodels, DT
from utils_act import target_names, value, unit, type, source, filters
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
dirs = f"{input_path}/activity.csv"
print(f"file input from {dirs}")

print("---Import event data---")
df = pd.read_csv(dirs)

if 'accopen1m' in df.columns and 'login1m' in df.columns:
    dfact = df[df['activity_type_name'] == 'Activation/Reactivation']
    dfact = dfact.drop(['login1m', 'login5m', 'login1h'], axis=1)
    dflog = df[df['activity_type_name'] == 'Login']
    dflog = dflog.drop(['accopen1m', 'accopen10m', 'accopen1h', 'accopen1d'], axis=1)
elif 'accopen1m' in df.columns and 'login1m' not in df.columns:
    dfact = df[df['activity_type_name'] == 'Activation/Reactivation']
    dflog = df[df['activity_type_name'] == 'Login']
elif 'accopen1m' not in df.columns and 'login1m' in df.columns:
    dflog = df[df['activity_type_name'] == 'Login']
    dfact = df[df['activity_type_name'] == 'Activation/Reactivation']
elif 'accopen1m' not in df.columns and 'login1m' not in df.columns:
    dflog = df[df['activity_type_name'] == 'Login']
    dfact = df[df['activity_type_name'] == 'Activation/Reactivation']
print(df.head(5))
print(df.shape)
print(dfact.shape)
print(dflog.shape)

#Procedure to check the dataset
rows_count = df.shape[0]
rows_countact = dfact.shape[0]
rows_countlog = dflog.shape[0]

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
elif anomaly*rows_countact < 10 and anomaly*rows_countlog < 10:
    json_rule = "[]"
    print(json_rule)
    fraud_rule = json.loads(json_rule)
    print(fraud_rule)        
    json_status = '{"status": "Insufficient input data. Try to add more data and higher anomaly percentage"}'
    print(json_status)
else:
    if anomaly*rows_countact >= 10 and anomaly*rows_countlog < 10:

        print("---Data preprocessing---")
        dpact = data_processor()
        dpact.objects
        # Fit model preprocessing
        dpact.process_train(dfact, model=anomalymodel)
        dpact.objects
        print(dpact.objects)

        # Old df with old columns
        print("---Dataframe after data preprocessing---")
        dffinalact = dpact.process_inference(dfact, model=anomalymodel)
        oldact = dffinalact.columns
        print("--Original dataframe--")
        print(dffinalact.head(5))
        print(dffinalact.shape)
        print(oldact)

        print("---Dataframe after data scaling---")
        scaleract = StandardScaler()
        scale_columnsact = dffinalact.select_dtypes(["int", "uint8", "float"]).columns.to_list()
        scalersact = scaleract.fit_transform(dffinalact[oldact].to_numpy())
        df_scaledact = pd.DataFrame(scalersact, index=dffinalact.index, columns=oldact)
        old_columnsact = df_scaledact.columns

        print(df_scaledact.head(5))
        print(df_scaledact.shape)
        print(old_columnsact)
        
        # New df with new columns due to correlation
        print("---Dataframe after feature selection---")
        if featsel == False:
            df_finalact = df_scaledact
        else:
            df_finalact = dpact.correlation(df_scaledact, 0.8)
        new_columnsact = df_finalact.columns
        print(df_finalact.head(5))
        print(df_finalact.shape)
        print(new_columnsact)
        
        ## Model ISO or MCD
        print("---Anomaly detection model---")
        modact = models()
        modact.models
        
        modact.model_train(df_finalact, model=anomalymodel, contamination=anomaly)
        modact.models
        
        # Obtain the data frame after addition of anomaly score and status
        print("---Output after the anomaly detection model---")
        dfsact = modact.model_inference(df_finalact, model=anomalymodel, genuine=genuines)
        outact=dfsact.loc[dfsact["outlier_status"]==-1]

        print(dfsact.head(5))
        print(dfsact.shape)
        print(outact.head(5))
        print(outact.shape)
        
        if outact.shape[0] < 10:
            json_rule = "[]"
            print(json_rule)
            fraud_rule = json.loads(json_rule)
            print(fraud_rule)
            json_status = '{"status": "Insufficient input data for login and no anomalies detected for activation"}'
            print(json_status)

        elif outact.shape[0] >= 10:

            ## Model RF for feature importance
            print("---Feature importance process---")
            rfmodact = rfmodels()
            rfmodact.rfmodels
        
            rfmodact.rfmodel_train(dfsact, model="rf")
            rfmodact.rfmodels
        
            featimportanceact = rfmodact.rfmodel_importance(dfsact, model="rf")

            # Inverse transform the dfs into the original df_ori, then transform only the selected new_columns in df_ori
            print("---New dataframe with selected columns for clustering process---")
            oriact = scaleract.inverse_transform(df_scaledact[old_columnsact])
            df_oriact = pd.DataFrame(oriact, columns=old_columnsact, index=dffinalact.index)

            print(df_oriact.head(5))
            print(df_oriact.shape)
            print(df_oriact.columns)
        
            scaleact = StandardScaler()
            scaledact = scaleact.fit_transform(df_oriact[new_columnsact].to_numpy())
            dfscaledact = pd.DataFrame(scaledact, index=dffinalact.index, columns=new_columnsact)
        
            print(dfscaledact.head(5))
            print(dfscaledact.shape)
            print(dfscaledact.columns)
        
            print("---Final dataframe for input of clustering---")
            # The new df with new columns, scores and anomalies 
            dfnewact = pd.concat([dfscaledact, dfsact['anomaly_score'], dfsact['outlier_status']], axis=1)
            outlieract=dfnewact.loc[dfnewact["outlier_status"]==-1]
            outlier_indexact=list(outlieract.index)

            print(dfnewact.head(5))
            print(dfnewact.shape)
            print(dfnewact.columns)
            print(outlieract.head(5))
            print(outlieract.shape)
            print(outlieract.columns)
        
            ## K-Means for clustering the outliers
            print("---K-Means clustering---")
            clustact = clustering()
            k_act = clustact.numbercluster(outlieract)

            if k_act == None:
                k_act = floor(outlieract.shape[0]/100)
            else:
                k_act = k_act

            outclustact = clustact.train(outlieract, k_act, new_columnsact)

            print(k_act)
            print(outclustact.head(5))
            print(outclustact.shape)
            print(outclustact.columns)
        
            ## Data prep for DT
            print("---Dataframe for input of decision tree---")
            dfoact = pd.concat([dfnewact, outclustact['label']], axis=1)
            print(dfoact.head(5))
            print(dfoact.shape)
            print(dfoact.columns)
        
            ## Decision Tree
            print("---Decision Tree process---")
            sm_tree_depths = range(1,3)
            sm_min_split = [100, 500, 1000]
            sm_min_leaf = [100, 500, 1000]

            dtinstanceact = DT()
            
            fraud_rule = dtinstanceact.training(dfoact, k_act, new_columnsact, sm_tree_depths, sm_min_split, sm_min_leaf, utils_act.target_names, utils_act.type, utils_act.source, utils_act.unit, utils_act.value, utils_act.filters, scaleact)
            
            json_status = '{"status": "Insufficient input data for login"}'
            print(json_status)

        print("---Fraud Rules---")
        print(f"===\n {fraud_rule}")


    elif anomaly*rows_countact < 10 and anomaly*rows_countlog >= 10:
        
        print("---Data preprocessing---")
        dplog = data_processor()
        dplog.objects
        # Fit model preprocessing
        dplog.process_train(dflog, model=anomalymodel)
        dplog.objects
        print(dplog.objects)

        # Old df with old columns
        print("---Dataframe after data preprocessing---")
        dffinallog = dplog.process_inference(dflog, model=anomalymodel)
        oldlog = dffinallog.columns
        print("--Original dataframe--")
        print(dffinallog.head(5))
        print(dffinallog.shape)
        print(oldlog)

        print("---Dataframe after data scaling---")
        scalerlog = StandardScaler()
        scale_columnslog = dffinallog.select_dtypes(["int", "uint8", "float"]).columns.to_list()
        scalerslog = scalerlog.fit_transform(dffinallog[oldlog].to_numpy())
        df_scaledlog = pd.DataFrame(scalerslog, index=dffinallog.index, columns=oldlog)
        old_columnslog = df_scaledlog.columns

        print(df_scaledlog.head(5))
        print(df_scaledlog.shape)
        print(old_columnslog)

        # New df with new columns due to correlation
        print("---Dataframe after feature selection---")
        if featsel == False:
            df_finallog = df_scaledlog
        else:
            df_finallog = dplog.correlation(df_scaledlog, 0.8)
        new_columnslog = df_finallog.columns
        print(df_finallog.head(5))
        print(df_finallog.shape)
        print(new_columnslog)

        ## Model ISO or MCD
        print("---Anomaly detection model---")
        modlog = models()
        modlog.models

        modlog.model_train(df_finallog, model=anomalymodel, contamination=anomaly)
        modlog.models

        # Obtain the data frame after addition of anomaly score and status
        print("---Output after the anomaly detection model---")
        
        dfslog = modlog.model_inference(df_finallog, model=anomalymodel, genuine=genuines)
        outlog=dfslog.loc[dfslog["outlier_status"]==-1]

        print(dfslog.head(5))
        print(dfslog.shape)
        print(outlog.head(5))
        print(outlog.shape)

        if outlog.shape[0] < 10:
            json_rule = "[]"
            print(json_rule)
            fraud_rule = json.loads(json_rule)
            print(fraud_rule)
            json_status = '{"status": "Insufficient input data for activation and no anomalies detected for login"}'
            print(json_status)
        elif outlog.shape[0] >= 10:
            ## Model RF for feature importance
            print("---Feature importance process---")
            rfmodlog = rfmodels()
            rfmodlog.rfmodels

            rfmodlog.rfmodel_train(dfslog, model="rf")
            rfmodlog.rfmodels

            featimportancelog = rfmodlog.rfmodel_importance(dfslog, model="rf")

            # Inverse transform the dfs into the original df_ori, then transform only the selected new_columns in df_ori
            print("---New dataframe with selected columns for clustering process---")
        
            orilog = scalerlog.inverse_transform(df_scaledlog[old_columnslog])
            df_orilog = pd.DataFrame(orilog, columns=old_columnslog, index=dffinallog.index)

            print(df_orilog.head(5))
            print(df_orilog.shape)
            print(df_orilog.columns)

            scalelog = StandardScaler()
            scaledlog = scalelog.fit_transform(df_orilog[new_columnslog].to_numpy())
            dfscaledlog = pd.DataFrame(scaledlog, index=dffinallog.index, columns=new_columnslog)

            print(dfscaledlog.head(5))
            print(dfscaledlog.shape)
            print(dfscaledlog.columns)

            print("---Final dataframe for input of clustering---")
            # The new df with new columns, scores and anomalies 
            dfnewlog = pd.concat([dfscaledlog, dfslog['anomaly_score'], dfslog['outlier_status']], axis=1)
            outlierlog=dfnewlog.loc[dfnewlog["outlier_status"]==-1]
            outlier_indexlog=list(outlierlog.index)

            print(dfnewlog.head(5))
            print(dfnewlog.shape)
            print(dfnewlog.columns)
            print(outlierlog.head(5))
            print(outlierlog.shape)
            print(outlierlog.columns)

            ## K-Means for clustering the outliers
            print("---K-Means clustering---")
        
            clustlog = clustering()
            k_log = clustlog.numbercluster(outlierlog)

            if k_log == None:
                k_log = floor(outlierlog.shape[0]/100)
            else:
                k_log = k_log

            outclustlog = clustlog.train(outlierlog, k_log, new_columnslog)
            print(k_log)
            print(outclustlog.head(5))
            print(outclustlog.shape)
            print(outclustlog.columns)

            ## Data prep for DT
            print("---Dataframe for input of decision tree---")
            dfolog = pd.concat([dfnewlog, outclustlog['label']], axis=1)
            print(dfolog.head(5))
            print(dfolog.shape)
            print(dfolog.columns)

            ## Decision Tree
            print("---Decision Tree process---")
            sm_tree_depths = range(1,3)
            sm_min_split = [100, 500, 1000]
            sm_min_leaf = [100, 500, 1000]

            dtinstancelog = DT()
            fraud_rule = dtinstancelog.training(dfolog, k_log, new_columnslog, sm_tree_depths, sm_min_split, sm_min_leaf, utils_act.target_names, utils_act.type, utils_act.source, utils_act.unit, utils_act.value, utils_act.filters, scalelog)

            json_status = '{"status": "Insufficient input data for activation"}'
            print(json_status)

        print("---Fraud Rules---")
        print(f"===\n {fraud_rule}")


    elif anomaly*rows_countact >= 10 and anomaly*rows_countlog >= 10:

        print("---Data preprocessing---")
        dpact = data_processor()
        dplog = data_processor()
        dpact.objects
        dplog.objects
        # Fit model preprocessing
        dpact.process_train(dfact, model=anomalymodel)
        dpact.objects
        dplog.process_train(dflog, model=anomalymodel)
        dplog.objects
        print(dpact.objects)
        print(dplog.objects)

        # Old df with old columns
        print("---Dataframe after data preprocessing---")
        dffinalact = dpact.process_inference(dfact, model=anomalymodel)
        oldact = dffinalact.columns

        dffinallog = dplog.process_inference(dflog, model=anomalymodel)
        oldlog = dffinallog.columns
        print("--Original dataframe--")
        print(dffinalact.head(5))
        print(dffinalact.shape)
        print(oldact)
        print(dffinallog.head(5))
        print(dffinallog.shape)
        print(oldlog)

        print("---Dataframe after data scaling---")
        scaleract = StandardScaler()
        scale_columnsact = dffinalact.select_dtypes(["int", "uint8", "float"]).columns.to_list()
        scalersact = scaleract.fit_transform(dffinalact[oldact].to_numpy())
        df_scaledact = pd.DataFrame(scalersact, index=dffinalact.index, columns=oldact)
        old_columnsact = df_scaledact.columns

        scalerlog = StandardScaler()
        scale_columnslog = dffinallog.select_dtypes(["int", "uint8", "float"]).columns.to_list()
        scalerslog = scalerlog.fit_transform(dffinallog[oldlog].to_numpy())
        df_scaledlog = pd.DataFrame(scalerslog, index=dffinallog.index, columns=oldlog)
        old_columnslog = df_scaledlog.columns

        print(df_scaledact.head(5))
        print(df_scaledact.shape)
        print(old_columnsact)
        print(df_scaledlog.head(5))
        print(df_scaledlog.shape)
        print(old_columnslog)

        # New df with new columns due to correlation
        print("---Dataframe after feature selection---")
        if featsel == False:
            df_finalact = df_scaledact
            df_finallog = df_scaledlog
        else:
            df_finalact = dpact.correlation(df_scaledact, 0.8)
            df_finallog = dplog.correlation(df_scaledlog, 0.8)
        new_columnsact = df_finalact.columns
        new_columnslog = df_finallog.columns
        print(df_finalact.head(5))
        print(df_finalact.shape)
        print(new_columnsact)
        print(df_finallog.head(5))
        print(df_finallog.shape)
        print(new_columnslog)

        ## Model ISO or MCD
        print("---Anomaly detection model---")
        modact = models()
        modact.models
        modlog = models()
        modlog.models

        modact.model_train(df_finalact, model=anomalymodel, contamination=anomaly)
        modact.models
        modlog.model_train(df_finallog, model=anomalymodel, contamination=anomaly)
        modlog.models

        # Obtain the data frame after addition of anomaly score and status
        print("---Output after the anomaly detection model---")
        dfsact = modact.model_inference(df_finalact, model=anomalymodel, genuine=genuines)
        outact=dfsact.loc[dfsact["outlier_status"]==-1]

        dfslog = modlog.model_inference(df_finallog, model=anomalymodel, genuine=genuines)
        outlog=dfslog.loc[dfslog["outlier_status"]==-1]

        print(dfsact.head(5))
        print(dfsact.shape)
        print(outact.head(5))
        print(outact.shape)
        print(dfslog.head(5))
        print(dfslog.shape)
        print(outlog.head(5))
        print(outlog.shape)

        if outact.shape[0] < 10 and outlog.shape[0] < 10:
            json_rule = "[]"
            print(json_rule)
            fraud_rule = json.loads(json_rule)
            print(fraud_rule)
            json_status = '{"status": "No anomalies detected for activation and login"}'
            print(json_status)

        elif outact.shape[0] >= 10 and outlog.shape[0] < 10:
            ## Model RF for feature importance
            print("---Feature importance process---")
            rfmodact = rfmodels()
            rfmodact.rfmodels
        
            rfmodact.rfmodel_train(dfsact, model="rf")
            rfmodact.rfmodels
        
            featimportanceact = rfmodact.rfmodel_importance(dfsact, model="rf")

            # Inverse transform the dfs into the original df_ori, then transform only the selected new_columns in df_ori
            print("---New dataframe with selected columns for clustering process---")
            oriact = scaleract.inverse_transform(df_scaledact[old_columnsact])
            df_oriact = pd.DataFrame(oriact, columns=old_columnsact, index=dffinalact.index)


            print(df_oriact.head(5))
            print(df_oriact.shape)
            print(df_oriact.columns)
        
            scaleact = StandardScaler()
            scaledact = scaleact.fit_transform(df_oriact[new_columnsact].to_numpy())
            dfscaledact = pd.DataFrame(scaledact, index=dffinalact.index, columns=new_columnsact)
        
            print(dfscaledact.head(5))
            print(dfscaledact.shape)
            print(dfscaledact.columns)
        
            print("---Final dataframe for input of clustering---")
            # The new df with new columns, scores and anomalies 
            dfnewact = pd.concat([dfscaledact, dfsact['anomaly_score'], dfsact['outlier_status']], axis=1)
            outlieract=dfnewact.loc[dfnewact["outlier_status"]==-1]
            outlier_indexact=list(outlieract.index)

            print(dfnewact.head(5))
            print(dfnewact.shape)
            print(dfnewact.columns)
            print(outlieract.head(5))
            print(outlieract.shape)
            print(outlieract.columns)
        
            ## K-Means for clustering the outliers
            print("---K-Means clustering---")
            clustact = clustering()
            k_act = clustact.numbercluster(outlieract)

            if k_act == None:
                k_act = floor(outlieract.shape[0]/100)
            else:
                k_act = k_act

            outclustact = clustact.train(outlieract, k_act, new_columnsact)

            print(k_act)
            print(outclustact.head(5))
            print(outclustact.shape)
            print(outclustact.columns)
        
            ## Data prep for DT
            print("---Dataframe for input of decision tree---")
            dfoact = pd.concat([dfnewact, outclustact['label']], axis=1)
            print(dfoact.head(5))
            print(dfoact.shape)
            print(dfoact.columns)
        
            ## Decision Tree
            print("---Decision Tree process---")
            sm_tree_depths = range(1,3)
            sm_min_split = [100, 500, 1000]
            sm_min_leaf = [100, 500, 1000]

            dtinstanceact = DT()
            
            fraud_rule = dtinstanceact.training(dfoact, k_act, new_columnsact, sm_tree_depths, sm_min_split, sm_min_leaf, utils_act.target_names, utils_act.type, utils_act.source, utils_act.unit, utils_act.value, utils_act.filters, scaleact)
            
            json_status = '{"status": "No anomalies detected for login"}'
            print(json_status)

        elif outact.shape[0] < 10 and outlog.shape[0] >= 10:
            ## Model RF for feature importance
            print("---Feature importance process---")
            rfmodlog = rfmodels()
            rfmodlog.rfmodels

            rfmodlog.rfmodel_train(dfslog, model="rf")
            rfmodlog.rfmodels

            featimportancelog = rfmodlog.rfmodel_importance(dfslog, model="rf")

            # Inverse transform the dfs into the original df_ori, then transform only the selected new_columns in df_ori
            print("---New dataframe with selected columns for clustering process---")
        
            orilog = scalerlog.inverse_transform(df_scaledlog[old_columnslog])
            df_orilog = pd.DataFrame(orilog, columns=old_columnslog, index=dffinallog.index)

            print(df_orilog.head(5))
            print(df_orilog.shape)
            print(df_orilog.columns)

            scalelog = StandardScaler()
            scaledlog = scalelog.fit_transform(df_orilog[new_columnslog].to_numpy())
            dfscaledlog = pd.DataFrame(scaledlog, index=dffinallog.index, columns=new_columnslog)

            print(dfscaledlog.head(5))
            print(dfscaledlog.shape)
            print(dfscaledlog.columns)

            print("---Final dataframe for input of clustering---")
            # The new df with new columns, scores and anomalies 
            dfnewlog = pd.concat([dfscaledlog, dfslog['anomaly_score'], dfslog['outlier_status']], axis=1)
            outlierlog=dfnewlog.loc[dfnewlog["outlier_status"]==-1]
            outlier_indexlog=list(outlierlog.index)

            print(dfnewlog.head(5))
            print(dfnewlog.shape)
            print(dfnewlog.columns)
            print(outlierlog.head(5))
            print(outlierlog.shape)
            print(outlierlog.columns)

            ## K-Means for clustering the outliers
            print("---K-Means clustering---")
        
            clustlog = clustering()
            k_log = clustlog.numbercluster(outlierlog)

            if k_log == None:
                k_log = floor(outlierlog.shape[0]/100)
            else:
                k_log = k_log

            outclustlog = clustlog.train(outlierlog, k_log, new_columnslog)
            print(k_log)
            print(outclustlog.head(5))
            print(outclustlog.shape)
            print(outclustlog.columns)

            ## Data prep for DT
            print("---Dataframe for input of decision tree---")
            dfolog = pd.concat([dfnewlog, outclustlog['label']], axis=1)
            print(dfolog.head(5))
            print(dfolog.shape)
            print(dfolog.columns)

            ## Decision Tree
            print("---Decision Tree process---")
            sm_tree_depths = range(1,3)
            sm_min_split = [100, 500, 1000]
            sm_min_leaf = [100, 500, 1000]

            dtinstancelog = DT()
            fraud_rule = dtinstancelog.training(dfolog, k_log, new_columnslog, sm_tree_depths, sm_min_split, sm_min_leaf, utils_act.target_names, utils_act.type, utils_act.source, utils_act.unit, utils_act.value, utils_act.filters, scalelog)

            json_status = '{"status": "No anomalies detected for activation"}'
            print(json_status)

        elif outact.shape[0] >= 10 and outlog.shape[0] >= 10:
            
            ## Model RF for feature importance
            print("---Feature importance process---")
            rfmodact = rfmodels()
            rfmodact.rfmodels
            rfmodlog = rfmodels()
            rfmodlog.rfmodels

            rfmodact.rfmodel_train(dfsact, model="rf")
            rfmodact.rfmodels
            rfmodlog.rfmodel_train(dfslog, model="rf")
            rfmodlog.rfmodels

            featimportanceact = rfmodact.rfmodel_importance(dfsact, model="rf")
            featimportancelog = rfmodlog.rfmodel_importance(dfslog, model="rf")

            # Inverse transform the dfs into the original df_ori, then transform only the selected new_columns in df_ori
            print("---New dataframe with selected columns for clustering process---")
            oriact = scaleract.inverse_transform(df_scaledact[old_columnsact])
            df_oriact = pd.DataFrame(oriact, columns=old_columnsact, index=dffinalact.index)

            orilog = scalerlog.inverse_transform(df_scaledlog[old_columnslog])
            df_orilog = pd.DataFrame(orilog, columns=old_columnslog, index=dffinallog.index)

            print(df_oriact.head(5))
            print(df_oriact.shape)
            print(df_oriact.columns)
            print(df_orilog.head(5))
            print(df_orilog.shape)
            print(df_orilog.columns)

            scaleact = StandardScaler()
            scaledact = scaleact.fit_transform(df_oriact[new_columnsact].to_numpy())
            dfscaledact = pd.DataFrame(scaledact, index=dffinalact.index, columns=new_columnsact)
            scalelog = StandardScaler()
            scaledlog = scalelog.fit_transform(df_orilog[new_columnslog].to_numpy())
            dfscaledlog = pd.DataFrame(scaledlog, index=dffinallog.index, columns=new_columnslog)

            print(dfscaledact.head(5))
            print(dfscaledact.shape)
            print(dfscaledact.columns)
            print(dfscaledlog.head(5))
            print(dfscaledlog.shape)
            print(dfscaledlog.columns)

            print("---Final dataframe for input of clustering---")
            # The new df with new columns, scores and anomalies 
            dfnewact = pd.concat([dfscaledact, dfsact['anomaly_score'], dfsact['outlier_status']], axis=1)
            outlieract=dfnewact.loc[dfnewact["outlier_status"]==-1]
            outlier_indexact=list(outlieract.index)

            dfnewlog = pd.concat([dfscaledlog, dfslog['anomaly_score'], dfslog['outlier_status']], axis=1)
            outlierlog=dfnewlog.loc[dfnewlog["outlier_status"]==-1]
            outlier_indexlog=list(outlierlog.index)

            print(dfnewact.head(5))
            print(dfnewact.shape)
            print(dfnewact.columns)
            print(outlieract.head(5))
            print(outlieract.shape)
            print(outlieract.columns)
            print(dfnewlog.head(5))
            print(dfnewlog.shape)
            print(dfnewlog.columns)
            print(outlierlog.head(5))
            print(outlierlog.shape)
            print(outlierlog.columns)

            ## K-Means for clustering the outliers
            print("---K-Means clustering---")
            clustact = clustering()
            k_act = clustact.numbercluster(outlieract)

            if k_act == None:
                k_act = floor(outlieract.shape[0]/100)
            else:
                k_act = k_act

            outclustact = clustact.train(outlieract, k_act, new_columnsact)

            clustlog = clustering()
            k_log = clustlog.numbercluster(outlierlog)

            if k_log == None:
                k_log = floor(outlierlog.shape[0]/100)
            else:
                k_log = k_log

            outclustlog = clustlog.train(outlierlog, k_log, new_columnslog)

            print(k_act)
            print(outclustact.head(5))
            print(outclustact.shape)
            print(outclustact.columns)
            print(k_log)
            print(outclustlog.head(5))
            print(outclustlog.shape)
            print(outclustlog.columns)

            ## Data prep for DT
            print("---Dataframe for input of decision tree---")
            dfoact = pd.concat([dfnewact, outclustact['label']], axis=1)
            dfolog = pd.concat([dfnewlog, outclustlog['label']], axis=1)
            print(dfoact.head(5))
            print(dfoact.shape)
            print(dfoact.columns)
            print(dfolog.head(5))
            print(dfolog.shape)
            print(dfolog.columns)

            ## Decision Tree
            print("---Decision Tree process---")
            sm_tree_depths = range(1,3)
            sm_min_split = [100, 500, 1000]
            sm_min_leaf = [100, 500, 1000]

            dtinstanceact = DT()
            dtinstancelog = DT()
            fraud_ruleact = dtinstanceact.training(dfoact, k_act, new_columnsact, sm_tree_depths, sm_min_split, sm_min_leaf, utils_act.target_names, utils_act.type, utils_act.source, utils_act.unit, utils_act.value, utils_act.filters, scaleact)
            fraud_rulelog = dtinstancelog.training(dfolog, k_log, new_columnslog, sm_tree_depths, sm_min_split, sm_min_leaf, utils_act.target_names, utils_act.type, utils_act.source, utils_act.unit, utils_act.value, utils_act.filters, scalelog)

            s1 = ''.join(str(x) for x in fraud_ruleact)
            s1new = s1.replace("'",'"')
            s2 = ''.join(str(x) for x in fraud_rulelog)
            s2new = s2.replace("'",'"')
            print(s1)
            print(s1new)
            print(s2)
            print(s2new)

            fraud_rules = ""
            fraud_rules = "[[" + s1new + "],[" + s2new + "]]"
            print(fraud_rules)

            fraud_rule = json.loads(fraud_rules)
            print(fraud_rule)

        print("---Fraud Rules---")
        print(f"===\n {fraud_rule}")


with open(f'{output_path}/activity.json','w') as f:
    rule_json = json.dumps(fraud_rule, separators=(',',': '))
    print(rule_json, file=f)

with open(f'{output_path}/activity_status.json','w') as f:
    status_json = json.dumps(json.loads(json_status))
    print(status_json, file=f)