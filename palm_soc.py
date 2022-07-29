#!/usr/bin/env python3.8
"""PALM - PV Active Load Manager."""

import sys
import time
import json
from datetime import datetime, timedelta
from typing import Tuple, List
import requests
import settings as stgs

# This software in any form is covered by the following Open Source BSD license:
#
# Copyright 2022, Steve Lewis
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted
# provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions
# and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of
# conditions and the following disclaimer in the documentation and/or other materials provided
# with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS
# OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
# AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

###########################################
# This code provides several functions:
# 1. Collection of generation/consumption data from GivEnergy API & upload to PVOutput
# 2. Load management - lights, excess power dumping
# 3. Setting overnight charge point, based on SolCast forecast & actual usage
#
###########################################

# Chamgelog:

# v0.8.3aSoC   28/Jul/22 Branch from palm - SoC only

PALM_VERSION = "v0.8.3aSoC"

#  Future improvements:
#
# -*- coding: utf-8 -*-

class GivEnergyObj:
    """Class for GivEnergy inverter"""

    def __init__(self):
        self.sys_status: List[str] = [""] * 5
        self.meter_status: List[str] = [""] * 5
        self.read_time_mins: int = -100
        self.line_voltage: float = 0
        self.grid_power: int = 0
        self.grid_energy: int = 0
        self.pv_power: int = 0
        self.pv_energy: int = 0
        self.batt_power: int = 0
        self.consumption: int = 0
        self.soc: int = 0
        self.base_load = stgs.GE.base_load

    def get_latest_data(self):
        """Download latest data from GivEnergy."""

        utc_timenow_mins = time_to_mins(time.strftime("%H:%M:%S", time.gmtime()))
        if (utc_timenow_mins > self.read_time_mins + 5 or
            utc_timenow_mins < self.read_time_mins):  # Update every 5 minutes plus day rollover

            url = stgs.GE.url + "system-data/latest"
            key = stgs.GE.key
            headers = {
                'Authorization': 'Bearer  ' + key,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }

            try:
                resp = requests.request('GET', url, headers=headers)
            except requests.exceptions.RequestException as error:
                print(error)
                return

            if len(resp.content) > 100:
                for index in range(4, -1, -1):  # Right shift old data
                    if index > 0:
                        self.sys_status[index] = self.sys_status[index - 1]
                    else:
                        try:
                            self.sys_status[index] = \
                                json.loads(resp.content.decode('utf-8'))['data']
                        except:
                            print("Error reading GivEnergy system status ", TIME_NOW_VAR)
                            print(resp.content)
                            self.meter_status[index] = self.meter_status[index + 1]
                if LOOP_COUNTER_VAR == 0:  # Pack array on startup
                    index = 1
                    while index < 5:
                        self.sys_status[index] = self.sys_status[0]
                        index += 1
                self.read_time_mins = time_to_mins(self.sys_status[0]['time'][11:])
                self.line_voltage = float(self.sys_status[0]['grid']['voltage'])
                self.grid_power = -1 * int(self.sys_status[0]['grid']['power'])  # -ve = export
                self.pv_power = int(self.sys_status[0]['solar']['power'])
                self.batt_power = int(self.sys_status[0]['battery']['power'])  # -ve = charging
                self.consumption = int(self.sys_status[0]['consumption'])
                self.soc = int(self.sys_status[0]['battery']['percent'])

            url = stgs.GE.url + "meter-data/latest"
            try:
                resp = requests.request('GET', url, headers=headers)
            except requests.exceptions.RequestException as error:
                print(error)
                return

            if len(resp.content) > 100:
                for index in range(4, -1, -1):  # Right shift old data
                    if index > 0:
                        self.meter_status[index] = self.meter_status[index - 1]
                    else:
                        try:
                            self.meter_status[index] = \
                                json.loads(resp.content.decode('utf-8'))['data']
                        except:
                            print("Error reading GivEnergy meter status ", TIME_NOW_VAR)
                            print(resp.content)
                            self.meter_status[index] = self.meter_status[index + 1]
                if LOOP_COUNTER_VAR == 0:  # Pack array on startup
                    index = 1
                    while index < 5:
                        self.meter_status[index] = self.meter_status[0]
                        index += 1
                self.pv_energy = int(self.meter_status[0]['today']['solar'] * 1000)

                # Daily grid energy cannot be <0 for PVOutput.org
                self.grid_energy = max(int(self.meter_status[0]['today']['consumption'] * 1000), 0)

    def get_load_hist(self, time_now_mins):
        """Download historical consumption data from GivEnergy and pack array for next SoC calc"""

        day_delta = 0 if (time_now_mins > 1430) else 1  # Use latest full day
        day = datetime.strftime(datetime.now() - timedelta(day_delta), '%Y-%m-%d')
        url = stgs.GE.url + "data-points/" + day
        key = stgs.GE.key
        headers = {
            'Authorization': 'Bearer  ' + key,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        params = {
        'page': '1',
        'pageSize': '2000',
        }

        try:
            resp = requests.request('GET', url, headers=headers, params=params)
        except requests.exceptions.RequestException as error:
            print(error)
            return

        if len(resp.content) > 100:
            history = json.loads(resp.content.decode('utf-8'))
            index = 0
            counter = 0
            current_energy = prev_energy = 0
            while index < 284:
                try:
                    current_energy = float(history['data'][index]['today']['consumption'])
                except:
                    break
                if counter == 0:
                    self.base_load[counter] = round(current_energy, 1)
                else:
                    self.base_load[counter] = round(current_energy - prev_energy, 1)
                counter += 1
                prev_energy = current_energy
                index += 12
            print("Info; Load Calc Summary:", current_energy, self.base_load)

    def set_inverter_register(self, register: str, value: str):
        """Set target charge for overnight charging."""

        url = stgs.GE.url + "settings/" + register + "/write"
        key = stgs.GE.key
        headers = {
            'Authorization': 'Bearer  ' + key,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        payload = {
            'value': value
        }

        resp = ""
        if not DEBUG_MODE:
            try:
                resp = requests.request('POST', url, headers=headers, json=payload)
            except requests.exceptions.RequestException as error:
                print(error)
                return

        if register == "64":
            reg_str = "(AC Charge 1 Start Time)"
        elif register == "71":
            reg_str = "(Battery Reserve)"
        elif register == "77":
            reg_str = "(AC Charge Upper % Limit)"
        else:
            reg_str = "(Unknown)"

        print("Info; Setting Register", register, reg_str, "to ", value, "Response:", resp)

    def set_soc(self, tgt_soc, batt_max_charge):
        """ Sets start time and target SoC for overnight charge."""

        end_time_mins = time_to_mins(stgs.GE.end_time)
        start_time_mins = time_to_mins(stgs.GE.start_time)
        batt_charge_mins = 60 * batt_max_charge / stgs.GE.charge_rate

        # Delay charge start to allow for any partial discharge
        charge_time_mins = batt_charge_mins * (tgt_soc - stgs.GE.batt_reserve) / 100
        discharge_time_mins = batt_charge_mins * (self.soc - tgt_soc) / 100
        start_time = time_to_hrs(max(start_time_mins + discharge_time_mins,
            end_time_mins - charge_time_mins))

        self.set_inverter_register("77", tgt_soc)
#       Comment out to see how battery discharges without adjusting start time
#        self.set_inverter_register("64", start_time)
#        self.set_inverter_register("71", tgt_soc)

    def restore_params(self):
        """Restore inverter parameters after overnight charge"""

        self.set_inverter_register("77", 100)
        self.set_inverter_register("64", stgs.GE.start_time)
        self.set_inverter_register("71", stgs.GE.batt_reserve)


    def compute_tgt_soc(self, gen_fcast, wgt_10: int, wgt_50: int, wgt_90: int, month: str, commit: bool):
        """Compute tomorrow's overnight SoC target"""

        batt_max_charge: float = stgs.GE.batt_max_charge

        tgt_soc = 100
        if gen_fcast.pv_est50_day[0] > 0:
            if month in stgs.GE.winter:
                print("info; winter month, SoC set to 100%")
            else:

                #  Step through forecast generation and consumption for coming day to identify
                #  lowest minimum before overcharge and maximum overcharge. If there is overcharge
                #  and the first min is above zero, reduce overnight charge for min export.

                batt_charge: float = [0] * 24
                batt_charge[0] = batt_max_charge
                max_charge = 0
                min_charge = batt_max_charge

                print()
                print("{:<20} {:>10} {:>10} {:>10}  {:>10} {:>10}".format("Info; SoC Calcs;",
                    "Hour", "Charge", "Cons", "Gen", "SoC"))

                index = 0
                while index < 24:
                    if index > 4:  # Battery is in Eco mode
                        total_load = ge.base_load[index]
                    else:  # Battery is in Charge mode
                        total_load = 0
                    est_gen = (gen_fcast.pv_est10_hrly[index] * wgt_10 +
                        gen_fcast.pv_est50_hrly[index] * wgt_50 +
                        gen_fcast.pv_est90_hrly[index] * wgt_90) / (wgt_10 + wgt_50 + wgt_90)
                    if index > 0:
                        batt_charge[index] = (batt_charge[index - 1] +
                            max(-1 * stgs.GE.charge_rate,
                                min(stgs.GE.charge_rate, est_gen - total_load)))
                        # Capture min charge on lowest down-slope before charge exceeds 100%
                        if (batt_charge[index] <= batt_charge[index - 1] and
                            max_charge < batt_max_charge):
                            min_charge = min(min_charge, batt_charge[index])
                        elif index > 4:  # Charging after overnight boost
                            max_charge = max(max_charge, batt_charge[index])

                    print("{:<20} {:>10} {:>10} {:>10}  {:>10} {:>10}".format("Info; SoC Calc;",
                        index, round(batt_charge[index], 2), round(total_load, 2),
                        round(est_gen, 2), int(100 * batt_charge[index] / batt_max_charge)))

                    index += 1

                max_charge_pcnt = int(100 * max_charge / batt_max_charge)
                min_charge_pcnt = int(100 * min_charge / batt_max_charge)

                #  Reduce nightly charge to capture max export
                if month in stgs.GE.shoulder:
                    tgt_soc = max(stgs.GE.max_soc_target, 130 - min_charge_pcnt,
                                  200 - max_charge_pcnt)
                else:
                    tgt_soc = max(stgs.GE.min_soc_target, 130 - min_charge_pcnt,
                                  200 - max_charge_pcnt)
                tgt_soc = int(min(max(tgt_soc, 0), 100))  # Limit range

                print()
                print("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("Info; SoC Calc Summary;",
                    "Max Charge", "Min Charge", "Max %", "Min %", "Target SoC"))
                print("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("Info; SoC Calc Summary;",
                    round(max_charge, 2), round(min_charge, 2),
                    max_charge_pcnt, min_charge_pcnt, tgt_soc))
                print("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("Info; SoC (Adjusted);",
                    round(max_charge, 2), round(min_charge, 2),
                    max_charge_pcnt - 100 + tgt_soc, min_charge_pcnt - 100 + tgt_soc, "\n"))

        else:
            print("Info; Incomplete Solcast data, setting target SoC to 100%")

        print("Info; SoC Summary; ", LONG_TIME_NOW_VAR, "; Tom Fcast Gen (kWh); ",
            gen_fcast.pv_est10_day[0], ";", gen_fcast.pv_est50_day[0], ";",
            gen_fcast.pv_est90_day[0], "; SoC Target (%); ", tgt_soc,
            "; Today Gen (kWh); ", round(self.pv_energy) / 1000, 2)

        if commit:
            self.set_soc(tgt_soc, batt_max_charge)

# End of GivEnergyObj() class definition

class SolcastObj:
    """Stores daily Solcast data."""

    def __init__(self):
        # Skeleton solcast summary array
        self.pv_est10_day: [int] = [0] * 7
        self.pv_est50_day: [int] = [0] *  7
        self.pv_est90_day: [int] = [0] * 7

        self.pv_est10_hrly: [int] = [0] * 24
        self.pv_est50_hrly: [int] = [0] * 24
        self.pv_est90_hrly: [int] = [0] * 24

    def update(self):
        """Updates forecast generation from Solcast."""

        def get_solcast(url) -> Tuple[bool, str]:
            """Download latest Solcast forecast."""

            solcast_url = url + stgs.Solcast.cmd + "&api_key=" + stgs.Solcast.key
            try:
                req = requests.get(solcast_url, timeout=5)
                req.raise_for_status()
            except requests.exceptions.RequestException as error:
                print(error)
                return False, ""

            if len(req.content) < 50:
                print("Warning: Solcast data missing/short")
                print(req.content)
                return False, ""

            solcast_data = json.loads(req.content.decode('utf-8'))
            return True, solcast_data
        #  End of get_solcast()

        # Download latest data for each array, abort if unsuccessful
        result, solcast_data_1 = get_solcast(stgs.Solcast.url_sw)
        if not result:
            print("Error; Problem reading Solcast data, using previous values (if any)")
            return

        result, solcast_data_2 = get_solcast(stgs.Solcast.url_se)
        if not result:
            print("Error; Problem reading Solcast data, using previous values (if any)")
            return

        print("Info; Successful Solcast download.")

        # Combine forecast for PV arrays & align data with day boundaries
        pv_est10 = [0] * 10080
        pv_est50 = [0] * 10080
        pv_est90 = [0] * 10080

        forecast_lines = min(len(solcast_data_1['forecasts']), len(solcast_data_2['forecasts']))
        interval = int(solcast_data_1['forecasts'][0]['period'][2:4])
        solcast_offset = (60 * int(solcast_data_1['forecasts'][0]['period_end'][11:13]) +
            int(solcast_data_1['forecasts'][0]['period_end'][14:16]) - interval - 60)

        index = solcast_offset
        cntr = 0
        while index < forecast_lines * interval:
            pv_est10[index] = (int(solcast_data_1['forecasts'][cntr]['pv_estimate10'] * 1000) +
                int(solcast_data_2['forecasts'][cntr]['pv_estimate10'] * 1000))

            pv_est50[index] = (int(solcast_data_1['forecasts'][cntr]['pv_estimate'] * 1000) +
                int(solcast_data_2['forecasts'][cntr]['pv_estimate'] * 1000))

            pv_est90[index] = (int(solcast_data_1['forecasts'][cntr]['pv_estimate90'] * 1000) +
                int(solcast_data_2['forecasts'][cntr]['pv_estimate90'] * 1000))

            if index > 1 and index % interval == 0:
                cntr += 1
            index += 1

        index = 0
        if solcast_offset > 720:  # Forget obout current day
            offset = 1440 - 90
        else:
            offset = 0

        while index < 7:  # Summarise daily forecasts
            start = index * 1440 + offset + 1
            end = start + 1439
            self.pv_est10_day[index] = round(sum(pv_est10[start:end]) / 60000, 3)
            self.pv_est50_day[index] = round(sum(pv_est50[start:end]) / 60000, 3)
            self.pv_est90_day[index] = round(sum(pv_est90[start:end]) / 60000, 3)
            index += 1

        index = 0
        while index < 24:  # Calculate hourly generation
            start = index * 60 + offset + 1
            end = start + 59
            self.pv_est10_hrly[index] = round(sum(pv_est10[start:end])/60000, 3)
            self.pv_est50_hrly[index] = round(sum(pv_est50[start:end])/60000, 3)
            self.pv_est90_hrly[index] = round(sum(pv_est90[start:end])/60000, 3)
            index += 1

        timestamp = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime())
        print("Info; PV Estimate 10% (hrly, 7 days) / kWh", ";", timestamp, ";",
            self.pv_est10_hrly[0:23], self.pv_est10_day[0:6])
        print("Info; PV Estimate 50% (hrly, 7 days) / kWh", ";", timestamp, ";",
            self.pv_est50_hrly[0:23], self.pv_est50_day[0:6])
        print("Info; PV Estimate 90% (hrly, 7 days) / kWh", ";", timestamp, ";",
            self.pv_est90_hrly[0:23], self.pv_est90_day[0:6])

# End of SolcastObj() class definition

def time_to_mins(time_in_hrs: str) -> int:
    """Convert times from HH:MM format to mins since midnight."""

    time_in_mins = 60 * int(time_in_hrs[0:2]) + int(time_in_hrs[3:5])
    return time_in_mins

#  End of time_to_mins()

def time_to_hrs(time_in: int) -> str:
    """Convert times from mins since midnight format to HH:MM."""

    hours = int(time_in // 60)
    mins = int(time_in - hours * 60)
    time_in_hrs = '{:02d}{}{:02d}'.format(hours, ":", mins)
    return time_in_hrs

#  End of time_to_hrs()

if __name__ == '__main__':

    # Current time definitions
    LOOP_COUNTER_VAR = 0
    LONG_TIME_NOW_VAR: str = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime())
    TIME_NOW_VAR: str = LONG_TIME_NOW_VAR[11:]
    TIME_NOW_MINS_VAR: int = time_to_mins(TIME_NOW_VAR)
    MONTH_VAR = LONG_TIME_NOW_VAR[3:5]

    print("Info; PALM... PV Automated Load Manager Version:", PALM_VERSION)

    # Parse any command-line arguments
    TEST_MODE: bool = False
    DEBUG_MODE: bool = False
    if len(sys.argv) > 1:
        if str(sys.argv[1]) in ["-t", "--test"]:
            TEST_MODE = True
            DEBUG_MODE = True
            print("Info; Running in test mode")
        elif str(sys.argv[1]) in ["-d", "--debug"]:
            DEBUG_MODE = True
            print("Info; Running in debug mode")

    # GivEnergy power object initialisation
    ge: GivEnergyObj = GivEnergyObj()
    ge.get_load_hist(TIME_NOW_MINS_VAR)

    # Solcast PV prediction object initialisation
    solcast: SolcastObj = SolcastObj()
    solcast.update()

    # compute & set SoC target
    print("Info; 10% forecast...")
    ge.compute_tgt_soc(solcast, 1, 0, 0, MONTH_VAR, False)
    print("Info; 50% forecast...")
    ge.compute_tgt_soc(solcast, 0, 1, 0, MONTH_VAR, False)
    print("Info; 90% forecast...")
    ge.compute_tgt_soc(solcast, 0, 0, 1, MONTH_VAR, False)
    print("Info; 1:2:0 weighted forecast...")
    ge.compute_tgt_soc(solcast, 1, 2, 0, MONTH_VAR, True)

# End
