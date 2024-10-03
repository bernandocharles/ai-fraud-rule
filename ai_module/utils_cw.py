#create a dictionary
target_names = [-1, 1]

value = {'transaction_amount': 0,
         'trxcw1m': 1, 
         'trxcw5m': 5, 
         'trxcw1h': 1, 
         'trxcw1d': 1,
         'inoutcw5m': 5, 
         'inoutcw30m': 30, 
         'inoutcw1h': 1, 
         'timecw_0_5': 0, 
         'timecw_0_3': 0, 
         'timecw_21_0': 0, 
         'velocitycw': 0}

print(value)

unit = {'transaction_amount': "seconds",
         'trxcw1m': "minutes", 
         'trxcw5m': "minutes", 
         'trxcw1h': "hours", 
         'trxcw1d': "days",
         'inoutcw5m': "minutes", 
         'inoutcw30m': "minutes", 
         'inoutcw1h': "hours", 
         'timecw_0_5': "seconds", 
         'timecw_0_3': "seconds", 
         'timecw_21_0': "seconds", 
         'velocitycw': "seconds"}

print(unit)

type = {'transaction_amount': "basic",
         'trxcw1m': "timeseries_trx_cnt_cw", 
         'trxcw5m': "timeseries_trx_cnt_cw", 
         'trxcw1h': "timeseries_trx_cnt_cw", 
         'trxcw1d': "timeseries_trx_cnt_cw",
         'inoutcw5m': "ratio_in_out_cw", 
         'inoutcw30m': "ratio_in_out_cw", 
         'inoutcw1h': "ratio_in_out_cw", 
         'timecw_0_5': "timerange_trx_cnt_cw", 
         'timecw_0_3': "timerange_trx_cnt_cw", 
         'timecw_21_0': "timerange_trx_cnt_cw", 
         'velocitycw': "velocity_cw"
       }

print(type)

source = {'transaction_amount': "tarik_tunai",
         'trxcw1m': "tarik_tunai", 
         'trxcw5m': "tarik_tunai", 
         'trxcw1h': "tarik_tunai", 
         'trxcw1d': "tarik_tunai",
         'inoutcw5m': "tarik_tunai", 
         'inoutcw30m': "tarik_tunai", 
         'inoutcw1h': "tarik_tunai", 
         'timecw_0_5': "tarik_tunai", 
         'timecw_0_3': "tarik_tunai", 
         'timecw_21_0': "tarik_tunai", 
         'velocitycw': "tarik_tunai"}

print(source)

filters = {'transaction_amount': "",
         'trxcw1m': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'trxcw5m': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'trxcw1h': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'trxcw1d': '{"value": {"transaction_id": ["$transaction_id","!="]}}',
         'inoutcw5m': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'inoutcw30m': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'inoutcw1h': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'timecw_0_5': '{"value": {"transaction_id": ["$transaction_id","!="],"transaction_time": [["00:00","05:00"],"between"]}}', 
         'timecw_0_3': '{"value": {"transaction_id": ["$transaction_id","!="],"transaction_time": [["00:00","03:00"],"between"]}}', 
         'timecw_21_0': '{"value": {"transaction_id": ["$transaction_id","!="],"transaction_time": [["21:00","23:59"],"between"]}}', 
         'velocitycw': '{"value": {"transaction_id": ["$transaction_id","!="]}}'}

print(filters)