from typing import Dict, Union
import os

def ml(source: str, model: str, params: Dict[str, Union[int, str]], input_path: str, output_path: str):
    final_params = f"-m {model} {json_params(params)}-i {input_path} -o {output_path}"
    
    if source == 'transfer':
        os.system(f"python3 /home/spark/sumut-ai/ai_module/main_tf.py {final_params}")
    if source == 'bill_payment':
        os.system(f"python3 /home/spark/sumut-ai/ai_module/main_bp.py {final_params}")
    if source == 'tarik_tunai':
        os.system(f"python3 /home/spark/sumut-ai/ai_module/main_cw.py {final_params}")
    if source == 'activity':
        os.system(f"python3 /home/spark/sumut-ai/ai_module/main_act.py {final_params}")
    if source == 'transaksi_internal':
        os.system(f"python3 /home/spark/sumut-ai/ai_module/main_int.py {final_params}")

def json_params(json):
    params = ""

    for key in json:
        if key == "f":
            params += f"-{key} "
        else:
            params += f"-{key} {json[key]} "
    
    return params

if __name__ == "__main__":
     ml("transfer", "iso", {"a": 10, "f": "true"}, "/home/spark/ai-rule-generator/generator-output/clqdk0gi000003569jh0er3wv/output/transfer", "/home/spark/ai-rule-generator/generator-output/clqdk0gi000003569jh0er3wv/output/rule")
    #ml("activity", "iso", {"a": 15, "f": "true"}, "/home/spark/ai-rule-generator/generator-output/clqdo1xba000035699apxln3q/output/activity", "/home/spark/ai-rule-generator/generator-output/clqdo1xba000035699apxln3q/output/rule")
    # ml("bill_payment", "iso", {"a": 10, "f": "true"}, "/home/spark/sumut-ai/etl/output/bill_payment", "/home/spark/sumut-ai/etl/output/rule")
    #ml("tarik_tunai", "iso", {"a": 10, "f": "true"}, "/home/spark/ai-rule-generator/generator-output/clqc2gzbu00003569dpx3ru56/output/tarik_tunai", "/home/spark/ai-rule-generator/generator-output/clqc2gzbu00003569dpx3ru56/output/rule")
    # ml("transaksi_internal", "iso", {"a": 10, "f": "true"}, "/home/spark/ai-rule-generator/generator-output/clqbuscrq00003569wzo5j5pv/output/transaksi_internal", "/home/spark/ai-rule-generator/generator-output/clqbuscrq00003569wzo5j5pv/output/rule")
