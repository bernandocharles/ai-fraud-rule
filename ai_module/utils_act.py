#create a dictionary
target_names = [-1, 1]

value = {'accopen1m': 1,
         'accopen10m': 10, 
         'accopen1h': 1, 
         'accopen1d': 1, 
         'login1m': 1,
         'login5m': 5, 
         'login1h': 1, 
         'velocityact': 0
         }

print(value)

unit = {'accopen1m': "minutes",
         'accopen10m': "minutes", 
         'accopen1h': "hours", 
         'accopen1d': "days", 
         'login1m': "minutes",
         'login5m': "minutes", 
         'login1h': "hours", 
         'velocityact': "seconds"
        }

print(unit)

type = {'accopen1m': "timeseries_type_act",
         'accopen10m': "timeseries_type_act",
         'accopen1h': "timeseries_type_act", 
         'accopen1d': "timeseries_type_act", 
         'login1m': "timeseries_login_act",
         'login5m': "timeseries_login_act", 
         'login1h': "timeseries_login_act", 
         'velocityact': "velocity_act"
       }

print(type)

source = {'accopen1m': "activity",
         'accopen10m': "activity", 
         'accopen1h': "activity", 
         'accopen1d': "activity", 
         'login1m': "activity",
         'login5m': "activity", 
         'login1h': "activity", 
         'velocityact': "activity"       
         }

print(source)

filters = {'accopen1m': '{"value": {"transaction_id": ["$transaction_id","!="]}}',
         'accopen10m': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'accopen1h': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'accopen1d': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'login1m': '{"value": {"transaction_id": ["$transaction_id","!="]}}',
         'login5m': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'login1h': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'velocityact': '{"value": {"transaction_id": ["$transaction_id","!="]}}'         
         }

print(filters)