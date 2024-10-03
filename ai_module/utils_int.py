#create a dictionary
target_names = [-1, 1]

value = {'correction': 0 }

print(value)

unit = {'correction': "seconds"}

print(unit)

type = {'correction': "correction_trx_int"}

print(type)

source = {'correction': "transaksi_internal"}

print(source)

filters = {'correction': '{"value": {"transaction_id": ["$transaction_id","!="]}}'}

print(filters)