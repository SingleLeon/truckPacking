# -*-coding:utf-8-*-
# !/usr/bin/env python3
# /opt/anaconda3/bin/python3.6
# @author:Shen Leon 
# Create:2019.08.17
# Revision:2019.08.17
"""
1.搭建计算框架基本思路：设定向量为m*n，货物设定为向量吗*n1，粒度选取最小单位；
2.车辆空间及重量足够时，优先装载，装载不了时，触发俄罗斯方块排序，确保装载率最优；
3.站点之间往返距离不同，优先考虑近的方向；
4.站点具备装载时间限制以及长度限制，优先选小车去限制严格的站点装货；
5.目标成本最优：发车费用以及站点间行驶费用，不考虑卸货时间；
"""
# update:

import pandas as pd
import numpy as np
import re
import time
import os
import json

# from do_cprofile import do_cprofile  # 提前结束 lc-20181018

# @do_cprofile('./PyCode/AutoFuel_run.prof')  # 提前结束 lc-20181018


def main_(folder_path):
    # *************1-计算初始化*************
    # *************1-0-获取文件*************
    file_name_list = os.listdir(folder_path)
    # *************1-1-获取列名*************
    # 信号筛选

    # 必备信号列表
    # sig_vin = ''
    sig_time = 'Time'
    sig_vehspd = 'VehSpdAvgDrvnHSC1'
    sig_accpos = 'EPTAccelActuPos_HSC1'
    sig_brkpos = 'BrkPdlPos_h1_HSC1'

    # 初始化结果列表
    trip_list = []

    # *************2-计算用户指标*************
    # 循环获取VIN len(car_id_group)
    for i in range(0, len(file_name_list)):
                
        index_time = time.time()
        print(file_name_list[i])
        # *************2-1-读取原始数据*************
        file_path = os.path.join(folder_path, file_name_list[i])
        # auto_data_raw = pd.read_csv(file_path, usecols=signal_name_list)
        auto_data_raw = pd.read_excel(file_path, sheet_name=0, header=0)
        # 去除含空字符对应行 样例数据中空行较多
        auto_data_dpna = auto_data_raw.dropna(axis=0, how='any')
        # 去除数据列重复，避免间隔有误；
        auto_data_ful = auto_data_dpna.drop_duplicates(subset=[sig_time, sig_vehspd], keep='first', inplace=False)

        print('Process car-id index ' + str(i) + ' time ...' + str(time.time() - index_time) + "s")
        
        if i == 10000:   # 提前结束 lc-20181018
            break
    
    # ************* 3-2结果导出 *************
    pd.DataFrame(trip_list).to_csv(folder_path + "All_result.csv", header=None, index=None)


# 主函数设置
if __name__ == '__main__':
    # *******1-GetData******
    start_time = time.time()
    stop_flag = 'False'
    folder_path = r'month3'
    bin_path = folder_path + r'\bin.json'
    matrix_path = folder_path + r'\matrix.json'
    station_path = folder_path + r'\station.json'
    vehicle_path = folder_path + r'\vehicle.json'
    with open(station_path, 'r') as load_station:
        load_dict = json.load(load_station)
        print(load_dict)
    # main_(folder_path)
    if stop_flag:
        load_dict['smallberg'] = [8200, {1:[['python', 81], ['shirt', 300]]}]
        print(load_dict)
        with open("../01_Result/record.json", "w") as dump_f:
            json.dump(load_dict, dump_f)
    print('Process Calculate time ...' + str(time.time() - start_time) + "s")
    # plt.show()
    print('Finish!')
