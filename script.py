"""
Input: path
Output: output.json 用完即删

用途是切分上交所和深交所的股票id并查找上一天的收盘价
必须指定日期，因为脚本无法判断递推一天后是否为交易日，牵涉到节假日问题。

Useage: python script.py <文件路径> <日期>
"""

import pandas as pd
import numpy as np
import os,os.path
import datetime
import json
import sys

def process_data(path):
    if not os.path.exists(path):
        print(f"The file {path} does not exist!")
        return

    date = pd.to_datetime(date_str)
    
    try:
        hfq_multi_daily = pd.read_parquet(path).reindex(index=[date]).fillna(method='ffill').replace(0,np.nan)
    except KeyError:
        print(f"The date {date_str} is not present in the data!")
        return
    
    sz = []
    sh = []
    sz_value = []
    sh_value = []
    all = hfq_multi_daily.columns.tolist()
    for id in all:
        if id[:2] == "30" or id[:2] == "00":
            sz_value.append(hfq_multi_daily[id].values[0])
            sz.append(id)
        else:
            sh_value.append(hfq_multi_daily[id].values[0])
            sh.append(id)
    dic = {"sz":sz, "sh":sh, "sz_closeprice":sz_value, "sh_closeprice":sh_value}
    with open("output.json", "w") as file:
        json.dump(dic, file)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <file_path> <date>")
        sys.exit(1)

    path = sys.argv[1]
    date_str = sys.argv[2]
    process_data(path, date_str)