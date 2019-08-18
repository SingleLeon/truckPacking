# -*-coding:utf-8-*-
# !/usr/bin/env python3
# /opt/anaconda3/bin/python3.6
# @author:Shen Leon 
# Create:2019.08.17
# Revision:2019.08.17
# update:   1.搭建计算框架

import pandas as pd
import numpy as np
import re
import time
import os

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
    # sig_steer = current_dbc['sig_steer'] if 'sig_steer' in current_dbc.keys() else ''
    # sig_accx = current_dbc['sig_accx'] if 'sig_accx' in current_dbc.keys() else ''
    # sig_accy = current_dbc['sig_accy'] if 'sig_accy' in current_dbc.keys() else ''
    # sig_accz = current_dbc['sig_accz'] if 'sig_accz' in current_dbc.keys() else ''
    sig_bmsvol = 'BMSPackVol_HSC1'
    sig_bmscrnt = 'BMSPackCrnt_HSC1'
    sig_bmssoc = 'BMSPackSOC'
    sig_tmspd = 'TMSpd_HSC1'
    sig_tmtoq = 'TMActuToq_HSC1'
    # 扩展信号列表
    # sig_ac = current_dbc['sig_ac'] if 'sig_ac' in current_dbc.keys() else ''
    # sig_acfanspd = current_dbc['sig_acfanspd'] if 'sig_acfanspd' in current_dbc.keys() else ''
    # sig_tempout = current_dbc['sig_tempout'] if 'sig_tempout' in current_dbc.keys() else ''
    # sig_tempin = current_dbc['sig_tempin'] if 'sig_tempin' in current_dbc.keys() else ''
    sig_odo = 'VehOdo_H1_HSC1'
    # sig_batt = current_dbc['sig_batt'] if 'sig_batt' in current_dbc.keys() else ''
    # sig_tempcool = current_dbc['sig_tempcool'] if 'sig_tempcool' in current_dbc.keys() else ''
    # sig_horn = current_dbc['sig_horn'] if 'sig_horn' in current_dbc.keys() else ''
    # sig_pwrmod = current_dbc['sig_pwrmod'] if 'sig_pwrmod' in current_dbc.keys() else ''
    # sig_date = current_dbc['sig_date'] if 'sig_date' in current_dbc.keys() else ''
    # sig_lightleft = current_dbc['sig_lightleft'] if 'sig_lightleft' in current_dbc.keys() else ''
    # sig_lightright = current_dbc['sig_lightright'] if 'sig_lightright' in current_dbc.keys() else ''
    # sig_raindetect = current_dbc['sig_raindetect'] if 'sig_raindetect' in current_dbc.keys() else ''
    # sig_wiperdetect = current_dbc['sig_wiperdetect'] if 'sig_wiperdetect' in current_dbc.keys() else ''
    # sig_nightdetect = current_dbc['sig_nightdetect'] if 'sig_nightdetect' in current_dbc.keys() else ''
    # 构造信号列表
    sig_bmspow = 'vehbmspackpow'
    sig_vehacc= 'veh_del_acc'
    sig_tmpow = 'veh_tmpow'
    sig_dcdcpow = 'veh_dcdc_pow'
    sig_force = 'veh_force'
    # 待扩展信号列表
    # sig_acdirect = 'vehaccircdirection'
    # sig_temptarget = 'vehacdrvtargettemp'
    # sig_raindetect = 'vehraindetected'
    sig_lvdcdccrnt = 'HVDCDCLVSideCrnt_HSC1'
    sig_lvdcdcvol = 'HVDCDCLVSideVol_HSC1'
    sig_hvdcdccrnt = 'HVDCDCHVSideCrnt_HSC1'
    sig_hvdcdcvol = 'HVDCDCHVSideVol_HSC1'
    signal_name_list = [sig_time, sig_vehspd, sig_accpos, sig_brkpos, sig_bmsvol,
                        sig_bmscrnt, sig_bmssoc, sig_tmspd, sig_tmtoq, sig_odo, sig_lvdcdccrnt, sig_lvdcdcvol,
                        sig_hvdcdccrnt, sig_hvdcdcvol]

    # 定义采样频率
    sample_hz = 0.1
    
    # 0-正常模式，1-Debug模式
    i_debug = 0
    
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

        # chunks = get_chunks(file_path,car_id_group)
        # 数据有效性判定 电压[250V,400V],电流[-30A,30A],时间[2018]

        # *************2-2-读取单个用户数据*************
        # 分块读取时使用 auto_data_ful = pd.concat(chunks[i],ignore_index=True)
        auto_data_car = auto_data_ful
        data_df = auto_data_car.sort_values(by=sig_time, axis=0, ascending=True).reset_index(drop="index")
        data_lens = len(data_df[sig_vehspd])
        # 对于数据样本异常，下一个循环
        if data_lens == 0:
            print(str(i) + 'test')
            continue

        # 构造信号列表
        data_df[sig_bmspow] = data_df[sig_bmsvol] * data_df[sig_bmscrnt]
        data_df[sig_dcdcpow] = data_df[sig_lvdcdccrnt] * data_df[sig_lvdcdcvol]
        
        # *************2-3-数据预计算*************
        # Base 加速度修正 后续考虑同时修正y
        
        # 负载情况-怠速无空调负载 若为空则为300
        trp_idlepow = data_df[(data_df[sig_vehspd] == 0)][sig_bmspow].mean()
        # 负载情况-怠速开空调负载 若为空则为1300
        trp_idlepowac = 0
        # 负载情况-DCDC 低压端输出
        trp_idledcdc = np.average(data_df[(data_df[sig_vehspd] == 0)][sig_lvdcdccrnt] * data_df[
            (data_df[sig_vehspd] == 0)][sig_lvdcdcvol])

        # ************* 2-4-行程基本参数 ***************
        if i == 0:
            trp_base_name = ["Car_Number", "Mileage(km)", "Odometer(km)", "Odo Ratio(%)", "Trip Time(h)",
                             "Average Speed(km/h)",  "Average Power Consumption(kWh/100km)",
                             "Average Driving Speed(km/h)", "Driving Power Consumption(kWh/100km)"]
        # 车号
        trp_vin = file_name_list[i]
        # 行驶总里程(km)
        trp_distance = sum(data_df[sig_vehspd]) * sample_hz / 3600
        # 里程表(km)
        trp_odo = data_df[sig_odo].max() - data_df[sig_odo].min()
        # 数据丢失率(%)
        trp_odoratio = 0
        odo_ratio = 100 * abs(trp_odo - trp_distance) / trp_odo
        
        # 行程时长(h)
        trp_time = data_lens * sample_hz / 3600
        # 行程均速(km/h)
        trp_vehspd = data_df[sig_vehspd].mean()
        # 行车均速(km/h)
        trp_avgvehspd = data_df[data_df[sig_vehspd] > 0][sig_vehspd].mean()
        
        # 总能耗(kWh)
        trp_power = sum(data_df[sig_bmspow]) * sample_hz / (3600 * 1000)
        # 行车能耗/空调能耗
        trp_drvpower = sum(data_df[data_df[sig_vehspd] > 0][sig_bmspow]) * sample_hz / (3600 * 1000)
        trp_drvpower_ac = 0

        if trp_distance > 0:
            # 平均能耗(kWh/100km)
            trp_avgpower = trp_power * 100 / trp_distance
            # 行车能耗(kWh/100km)
            trp_avgdrvpower = (trp_drvpower - trp_drvpower_ac) * 100 / trp_distance
        else:
            trp_avgpower = 0
            trp_avgdrvpower = 0
        # 基础行程参数列表
        trp_base_list = [trp_vin, trp_distance, trp_odo, trp_odoratio, trp_time, trp_vehspd, trp_avgpower,
                         trp_avgvehspd, trp_avgdrvpower]
        
        # ************* 2-5-能耗分布参数 *************
        if i == 0:
            trp_sumpower_name = ["Output Power of Battery(kWh)", "Power Recovery(kWh)",
                                 "Power of low-voltage load(kWh)", "Air Conditioning Power Consumption(kWh)",
                                 "Output Power of Motor(kWh)", "Ratio of Power Recovery(%)",
                                 "Ratio of Power Conversion(%)", "Ratio of low voltage load(%)",
                                 "Ratio of Air Conditioning(%)"]
        
        # 电池输出总能量(kWh)
        trp_batpow = (data_df[data_df[sig_bmscrnt] >= 0][sig_bmspow].sum()) * sample_hz / (3600 * 1000)
        # 能量回收(kWh)
        trp_recover = abs(sum(data_df[(data_df[sig_vehspd] > 2) & (data_df[sig_bmscrnt] < 0)][sig_bmspow])
                          ) * sample_hz / (3600 * 1000)
        # 低压负载总量(kWh)
        trp_lowvolload = trp_idlepow * sample_hz / (3600 * 1000)
        # 空调能耗(kWh) # 能耗计算-空调能耗
        trp_acpow = 0
        # 电机总输出(kWh)
        trp_tmpow = sum(data_df[sig_tmspd] * data_df[sig_tmtoq]) * sample_hz / (9.55 * 3600 * 1000)
        # 能耗分布参数
        try:
            trp_pecnt_recover = 100 * trp_recover / trp_batpow
            trp_pecnt_transpow = 100 * (trp_batpow - trp_lowvolload - trp_tmpow - trp_acpow) / trp_batpow
            trp_pecnt_lowload = 100 * trp_lowvolload / trp_batpow
            trp_pecnt_acpow = 100 * trp_acpow / trp_batpow
        except ZeroDivisionError:
            trp_pecnt_recover = 0
            trp_pecnt_transpow = 0
            trp_pecnt_lowload = 0
            trp_pecnt_acpow = 0

        # 能耗分布列表
        trp_sumpower_list = [trp_batpow, trp_recover, trp_lowvolload, trp_acpow, trp_tmpow, trp_pecnt_recover,
                             trp_pecnt_transpow, trp_pecnt_lowload, trp_pecnt_acpow]

        # ************* 2-6-能耗速度区间分布参数 *************
        vehspd_grid = [0, 20, 40, 60, 80, 100, 0, 40, 80]
        trp_vehdist_list = []
        # trp_vehdist_name = ["0~20 Averge Power Consumption(kwh/100km)",  "0~20 Power(kWh)", "0~20 Mileage(km)", ...]
        if i == 0:
            trp_vehdist_name = []
            for i_vehspd in range(len(vehspd_grid)):
                if (i_vehspd < len(vehspd_grid)-1) and (vehspd_grid[i_vehspd] < vehspd_grid[i_vehspd + 1]):
                    str_vehspd_name = str(vehspd_grid[i_vehspd]) + '~' + str(vehspd_grid[i_vehspd + 1])
                else:
                    str_vehspd_name = ">" + str(vehspd_grid[i_vehspd])
                trp_vehdist_name.append(str_vehspd_name + ' Averge Power Consumption(kwh/100km)')
                trp_vehdist_name.append(str_vehspd_name + ' Power(kWh)')
                trp_vehdist_name.append(str_vehspd_name + ' Mileage(km)')

        for i_vehspd in range(len(vehspd_grid)):
            if (i_vehspd < len(vehspd_grid) - 1) and (vehspd_grid[i_vehspd] < vehspd_grid[i_vehspd + 1]):
                trp_batpow_spd = (sum(data_df[(data_df[sig_vehspd] >= vehspd_grid[i_vehspd]) &
                                              (data_df[sig_vehspd] < vehspd_grid[i_vehspd + 1])][sig_bmspow])
                                  ) * sample_hz / (3600 * 1000)
                trp_distance_spd = (sum(data_df[(data_df[sig_vehspd] >= vehspd_grid[i_vehspd]) &
                                                (data_df[sig_vehspd] < vehspd_grid[i_vehspd + 1])][sig_vehspd])
                                    ) * sample_hz / (3.6 * 1000)

            else:
                trp_batpow_spd = (sum(data_df[(data_df[sig_vehspd] >= vehspd_grid[i_vehspd])][sig_bmspow])
                                  ) * sample_hz / (3600 * 1000)
                trp_distance_spd = (sum(data_df[(data_df[sig_vehspd] >= vehspd_grid[i_vehspd])][sig_vehspd])
                                    ) * sample_hz / (3.6 * 1000)
            try:
                trp_vehdist_list.append(100 * trp_batpow_spd / trp_distance_spd)
            except ZeroDivisionError:
                trp_vehdist_list.append(0)
            trp_vehdist_list.append(trp_batpow_spd)
            trp_vehdist_list.append(trp_distance_spd)

        # ************* 2-7-行程统计参数 *************
        if i == 0:
            trp_keyindex_name = ["Ratio of Idle Time(%)", "DCDC power(W)",
                                 "External Charging(kWh)", "Idle Low-voltage Load without AC(W)",
                                 "Idle DCDC power(W)", "Ratio of AC ON(%)",
                                 "Average Temperature Range during AC ON(℃)", "Average Acc Pedal(%)",
                                 "Average Brake Pedal(%)"]

        # 怠速比例((%)
        trp_idle = 100 * len(data_df[data_df[sig_vehspd] == 0][sig_vehspd]) / data_lens
        # # 回收平均减速度(g)
        # trp_coastacc = 0
        # DCDC平均功率
        trp_dcdcpow = np.average(data_df[sig_lvdcdccrnt] * data_df[sig_lvdcdcvol])
        # 外接充电(kWh)
        trp_charge = 0

        # 空调时间占比(%)
        trp_pecnt_actime = 0
        # 空调运行时平均温差(℃)
        trp_acdelttemp = 0
        # 油门平均开度(%)
        trp_accpos = data_df[data_df[sig_vehspd] > 0][sig_accpos].mean()
        # 制动平均开度(%)
        trp_brkpos = data_df[data_df[sig_vehspd] > 0][sig_brkpos].mean()
        
        # 行程统计参数列表
        trp_keyindex_list = [trp_idle, trp_dcdcpow, trp_charge, trp_idlepow, trp_idledcdc,
                             trp_pecnt_actime, trp_acdelttemp, trp_accpos, trp_brkpos]
        
        # 扩展：整车三电参数："BMS平均放电电压(V)", "BMS平均充电电压(V)", "BMS平均放电电流(A)", "BMS平均充电电流(A)",
        # "平均能量回收电流(A)", "BMS平均SOC(%)", "电机平均转速(rpm)", "电机平均扭矩(Nm)",

        # ************* 2-10 驾驶行为指标 *************
        # 需求输入(车号、车速、X方向加速度、油门位置)
        
        if i == 0:
            trp_driveindex_name = ['Max Vehspd(km/h)', 'Vehspd <30(%)', 'Vehspd 30-60(%)', 'Vehspd 60-90(%)',
                                   'Vehspd 90-120(%)', 'Vehspd >120(%)', 'SOC_Start', 'SOC_End']

        # table=pd.read_csv("./data/tmp_dtsvc_293_f500_17_0.csv", usecols=['vin','vehspeed','tboxaccelx','accelactupos'])
        # #导入表格
        # cus_name=np.unique(table['vin'])
        # cus_num=np.shape(cus_name)[0]
        # len_cus = np.zeros(cus_num)
        # #确认试验人员的名字和数量
        # Behave_data_char=np.zeros([15,cus_num])

        # 最大速度
        try:
            trp_maxspd = max(data_df[sig_vehspd])
        except:
            trp_maxspd = 0
          
         # 小于30的速度比例
        trp_pecnt_veh30 = sum((data_df[sig_vehspd] > 0) & (data_df[sig_vehspd] <= 30)) / data_lens
        # 30-60的速度比例
        trp_pecnt_veh3060 = sum((data_df[sig_vehspd] > 30) & (data_df[sig_vehspd] <= 60)) / data_lens
        # 60-90的速度比例
        trp_pecnt_veh6090 = sum((data_df[sig_vehspd] > 60) & (data_df[sig_vehspd] <= 90)) / data_lens
        # 90-120的速度比例
        trp_pecnt_veh90120 = sum((data_df[sig_vehspd] > 90) & (data_df[sig_vehspd] <= 120)) / data_lens
        # >120的速度比例
        trp_pecnt_veh120 = sum(data_df[sig_vehspd] > 120) / data_lens
        # SOC_Start
        trp_soc_start = max(data_df[sig_bmssoc])
        # SOC_End
        trp_soc_end = min(data_df[sig_bmssoc])

        # table_accpos = data_df[data_df[sig_accpos] > 0]
        # table_accpos = np.array(table_accpos[sig_accpos])
        # data_lens = len(table_accpos[sig_accpos])
        # accpos_var = np.zeros(data_lens-1)
        # #油门踏板差值
        # for j in range(data_lens-1):
        #     accpos_var[j]=abs(table_accpos[j+1]-table_accpos[j])
        #  #平均加速踏板
        # Behave_data_char[15][cus] = np.mean(table_accpos[sig_accpos])           
        # #油门踏板方差
        # Behave_data_char[16][cus] = np.std(table_accpos[sig_accpos])             
        # #平均加速踏板变化率
        # Behave_data_char[17][cus] = np.mean(accpos_var)                              
        # #加速踏板变化方差
        # Behave_data_char[18][cus] = np.std(accpos_var)                               

        trp_driveindex_list = [trp_maxspd, trp_pecnt_veh30, trp_pecnt_veh3060, trp_pecnt_veh6090, trp_pecnt_veh90120, 
            trp_pecnt_veh120, trp_soc_start, trp_soc_end]

        # ************* 2-11 稳速区间阻力指标 *************
        vehspd_const = [15, 32, 50, 70, 100]
        trp_force_list = []
        if i == 0:
            # trp_force_name = ['16km/h VehSpd(km/h)', '16km/h Force(N)', ...]
            trp_force_name = []
            for i_vehspd in range(len(vehspd_const)):
                trp_force_name.append(str(vehspd_const[i_vehspd]) + 'km/h VehSpd(km/h)')
                trp_force_name.append(str(vehspd_const[i_vehspd]) + 'km/h Acc Force(N)')
                trp_force_name.append(str(vehspd_const[i_vehspd]) + 'km/h ABC Force(N)')
                trp_force_name.append(str(vehspd_const[i_vehspd]) + 'km/h STD Force(N)')
                trp_force_name.append(str(vehspd_const[i_vehspd]) + 'km/h TM Force(N)')

        # 构造信号列表 滑行929 A 88.22 B0.797 C0.03114
        data_df[sig_vehacc] = (data_df[sig_vehspd].diff()) / sample_hz
        data_df[sig_tmpow] = (data_df[sig_tmspd] * data_df[sig_tmtoq]) / 9.55
        data_df[sig_force] = 88.22 + data_df[sig_vehspd] * 0.797 + data_df[sig_vehspd] * data_df[
            sig_vehspd] * 0.03114 + 1029 * data_df[sig_vehacc] / 3.6

        for i_vehspd in range(len(vehspd_const)):
            trp_vehspd_const = data_df[(data_df[sig_vehspd] > vehspd_const[i_vehspd] - 2) & (
                data_df[sig_vehspd] < vehspd_const[i_vehspd] + 2) & (abs(data_df[sig_vehacc]) < 2) & (
                data_df[sig_accpos] > 0) & (data_df[sig_tmtoq] > 0)][sig_vehspd].mean()
            trp_acc = data_df[(data_df[sig_vehspd] > vehspd_const[i_vehspd] - 2) & (
                    data_df[sig_vehspd] < vehspd_const[i_vehspd] + 2) & (abs(data_df[sig_vehacc]) < 2) & (
                    data_df[sig_accpos] > 0) & (data_df[sig_tmtoq] > 0)][sig_vehacc].mean()
            trp_acc_force = trp_acc * 1029 / 3.6
            trp_abc_force = data_df[(data_df[sig_vehspd] > vehspd_const[i_vehspd] - 2) & (
                    data_df[sig_vehspd] < vehspd_const[i_vehspd] + 2) & (abs(data_df[sig_vehacc]) < 2) & (
                    data_df[sig_accpos] > 0) & (data_df[sig_tmtoq] > 0)][sig_force].mean()
            trp_std_force = 88.22 + trp_vehspd_const * 0.797 + trp_vehspd_const * trp_vehspd_const * 0.03114
            try:
                trp_tm_force = (data_df[(data_df[sig_vehspd] > (vehspd_const[i_vehspd] - 2)) & (
                        data_df[sig_vehspd] < (vehspd_const[i_vehspd] + 2)) & (abs(data_df[sig_vehacc]) < 2) & (
                        data_df[sig_accpos] > 0) & (data_df[sig_tmtoq] > 0)][sig_tmpow].mean()) / (
                        trp_vehspd_const / 3.6)
            except ZeroDivisionError:
                trp_tm_force = 0
            trp_force_list.append(trp_vehspd_const)
            trp_force_list.append(trp_acc_force)
            trp_force_list.append(trp_abc_force)
            trp_force_list.append(trp_std_force)
            trp_force_list.append(trp_tm_force)

            # ************* 2-12 效率指标 *************
            if i == 0:
                trp_effi_name = ['TM Output(%)', 'TM Regen(%)']
            # 电机驱动效率(%)
            trp_tm_out = data_df[data_df[sig_tmtoq] > 0][sig_tmpow].mean()/(
                    data_df[data_df[sig_tmtoq] > 0][sig_bmspow].mean() - data_df[data_df[sig_tmtoq] > 0][sig_dcdcpow].mean())

            # 电机回收效率(%)
            trp_tm_regen = abs(data_df[data_df[sig_tmtoq] < 0][sig_bmspow].mean() - data_df[data_df[sig_tmtoq] < 0][
                sig_dcdcpow].mean()) / abs(data_df[data_df[sig_tmtoq] < 0][sig_tmpow].mean())

            # 行程统计参数列表
            trp_effi_list = [trp_tm_out, trp_tm_regen]

        # ************* 3-1结果封装 *************
        if i == 0:
            trip_list.append(trp_base_name + trp_sumpower_name + trp_vehdist_name + trp_keyindex_name +
                             trp_driveindex_name + trp_force_name + trp_effi_name)
        
        # 判断行驶里程大于2km
        if trp_distance > 2:
            # 行程计算值汇总
            trip_list.append(trp_base_list + trp_sumpower_list + trp_vehdist_list + trp_keyindex_list +
                             trp_driveindex_list + trp_force_list + trp_effi_list)
                             
        print('Process car-id index ' + str(i) + ' time ...' + str(time.time() - index_time) + "s")
        
        if i == 10000:   # 提前结束 lc-20181018
            break
    
    # ************* 3-2结果导出 *************
    pd.DataFrame(trip_list).to_csv(folder_path + "All_result.csv", header=None, index=None)


# 主函数设置
if __name__ == '__main__':
    # *******1-GetData******
    start_time = time.time()
    # tmp_dtsvc_293_f500_17_1HS16528_2018-09-17_0917_data
    folder_path = r'D:\7_TestData\EX21\EX21_RangeTest_20190527\cleandata'
    # main_(folder_path)
    print('Process Calculate time ...' + str(time.time() - start_time) + "s")
    # plt.show()
    print('Finish!')
