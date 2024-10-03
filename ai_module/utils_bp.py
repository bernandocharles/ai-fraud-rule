#create a dictionary
target_names = [-1, 1]

value = {'transaction_amount': 0,
         'trxdest1m': 1, 
         'trxdest10m': 10, 
         'trxdest1h': 1, 
         'trxdest1d': 1,
         'timebp_0_5': 0, 
         'timebp_0_3': 0, 
         'timebp_21_0': 0, 
         'amtratiobp30d': 30, 
         'amtratiobp90d': 90
         }

print(value)

unit = {'transaction_amount': "seconds",
        'trxdest1m': "minutes", 
         'trxdest10m': "minutes", 
         'trxdest1h': "hours", 
         'trxdest1d': "days",
         'timebp_0_5': "seconds", 
         'timebp_0_3': "seconds", 
         'timebp_21_0': "seconds", 
         'amtratiobp30d': "days", 
         'amtratiobp90d': "days"
        }

print(unit)

type = {'transaction_amount': "basic",
        'trxdest1m': "timeseries_trx_cnt_bp", 
         'trxdest10m': "timeseries_trx_cnt_bp", 
         'trxdest1h': "timeseries_trx_cnt_bp", 
         'trxdest1d': "timeseries_trx_cnt_bp",
         'timebp_0_5': "timerange_trx_cnt_bp", 
         'timebp_0_3': "timerange_trx_cnt_bp", 
         'timebp_21_0': "timerange_trx_cnt_bp", 
         'amtratiobp30d': "ratio_avg_trx_amt_bp", 
         'amtratiobp90d': "ratio_avg_trx_amt_bp"
       }

print(type)

source = {'transaction_amount': "bill_payment",
         'trxdest1m': "bill_payment", 
         'trxdest10m': "bill_payment", 
         'trxdest1h': "bill_payment", 
         'trxdest1d': "bill_payment",
         'timebp_0_5': "bill_payment", 
         'timebp_0_3': "bill_payment", 
         'timebp_21_0': "bill_payment", 
         'amtratiobp30d': "bill_payment", 
         'amtratiobp90d': "bill_payment"         
         }

print(source)

filters = {'transaction_amount': "",
         'trxdest1m': '{"value": {"transaction_id": ["$transaction_id","!="],"transaction_type_name": [["Top Up OVO","Top Up GoPay","Top Up Paytren","Pembayaran XL Postpaid","Pembelian XL Paket Data","Pembelian Pulsa Internet Telkomsel","Pembelian XL Postpaid","Pembelian Indosat Prepaid","Pembayaran Indosat Postpaid","Pembelian XL Prepaid","Pembelian Pulsa Kartu AS & Simpati","Pembayaran Tagihan Kartu Halo"],"in"],"dest_phone_number": ["$dest_phone_number","=="]}}', 
         'trxdest10m': '{"value": {"transaction_id": ["$transaction_id","!="],"transaction_type_name": [["Top Up OVO","Top Up GoPay","Top Up Paytren","Pembayaran XL Postpaid","Pembelian XL Paket Data","Pembelian Pulsa Internet Telkomsel","Pembelian XL Postpaid","Pembelian Indosat Prepaid","Pembayaran Indosat Postpaid","Pembelian XL Prepaid","Pembelian Pulsa Kartu AS & Simpati","Pembayaran Tagihan Kartu Halo"],"in"],"dest_phone_number": ["$dest_phone_number","=="]}}', 
         'trxdest1h': '{"value": {"transaction_id": ["$transaction_id","!="],"transaction_type_name": [["Top Up OVO","Top Up GoPay","Top Up Paytren","Pembayaran XL Postpaid","Pembelian XL Paket Data","Pembelian Pulsa Internet Telkomsel","Pembelian XL Postpaid","Pembelian Indosat Prepaid","Pembayaran Indosat Postpaid","Pembelian XL Prepaid","Pembelian Pulsa Kartu AS & Simpati","Pembayaran Tagihan Kartu Halo"],"in"],"dest_phone_number": ["$dest_phone_number","=="]}}', 
         'trxdest1d': '{"value": {"transaction_id": ["$transaction_id","!="],"transaction_type_name": [["Top Up OVO","Top Up GoPay","Top Up Paytren","Pembayaran XL Postpaid","Pembelian XL Paket Data","Pembelian Pulsa Internet Telkomsel","Pembelian XL Postpaid","Pembelian Indosat Prepaid","Pembayaran Indosat Postpaid","Pembelian XL Prepaid","Pembelian Pulsa Kartu AS & Simpati","Pembayaran Tagihan Kartu Halo"],"in"],"dest_phone_number": ["$dest_phone_number","=="]}}',
         'timebp_0_5': '{"value": {"transaction_id": ["$transaction_id", "!="],"transaction_time": [["00:00","05:00"],"between"]}}', 
         'timebp_0_3': '{"value": {"transaction_id": ["$transaction_id", "!="],"transaction_time": [["00:00","03:00"],"between"]}}', 
         'timebp_21_0': '{"value": {"transaction_id": ["$transaction_id", "!="],"transaction_time": [["21:00","23:59"],"between"]}}', 
         'amtratiobp30d': '{"value": {"transaction_id": ["$transaction_id","!="]}}', 
         'amtratiobp90d': '{"value": {"transaction_id": ["$transaction_id","!="]}}'         
         }

print(filters)