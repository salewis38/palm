#!/usr/bin/env python3.8
"""PALM - PV Active Load Manager."""

import sys
import time
import threading
import json
from datetime import datetime, timedelta
from typing import Tuple, List
from urllib.parse import urlencode
# from pprint import pprint
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
# v0.6.0    12/Feb/22 First cut at GivEnergy interface
# v0.7.0/1  15/Apr/22 Reduced verbosity, revised forecasting
# v0.7.2    05/May/22 Added actual load to SoC forecast
# v0.7.3    05/May/22 Removed redundant Accumulator feature, charge limiting, p-printing
# v0.8.0    08/May/22 Moved SoCcalc to GivEnergy, settings.py replaces palm_Config.json
# v0.8.1    15/May/22 Rationalised sunset in env_obj, compensated for AC Charge discharge

PALM_VERSION = "v0.8.1"

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
        if utc_timenow_mins > self.read_time_mins + 5 or\
            utc_timenow_mins < self.read_time_mins:  # Update every 5 minutes plus day rollover

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
                        self.sys_status[index] = json.loads(resp.content.decode('utf-8'))['data']
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
                        self.meter_status[index] = json.loads(resp.content.decode('utf-8'))['data']
                if LOOP_COUNTER_VAR == 0:  # Pack array on startup
                    index = 1
                    while index < 5:
                        self.meter_status[index] = self.meter_status[0]
                        index += 1
                self.pv_energy = int(self.meter_status[0]['today']['solar'] * 1000)
                self.grid_energy = int(self.meter_status[0]['today']['consumption'] * 1000)

    def get_load_hist(self):
        """Download historical consumption data from GivEnergy and pack array for next SoC calc"""

        day_delta = 0 if (TIME_NOW_MINS_VAR > 1430) else 1  # Use latest full day
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
        elif register == "77":
            reg_str = "(AC Charge Upper % Limit)"
        else:
            reg_str = "Unknown"

        print("Info; Setting Register", register, reg_str, "to ", value, "Response:", resp)

    def compute_tgt_soc(self, gen_fcast) -> int:
        """Compute tomorrow's overnight SoC target"""

        batt_max_charge: float = stgs.GE.batt_capacity * 0.84  # Usable battery capacity

        month = LONG_TIME_NOW_VAR[3:5]

        tgt_soc = 100
        if gen_fcast.pv_est50_day[0] > 0:
            if month in stgs.GE.winter:
                print("Info; Winter month: target SoC set to 100%")
            elif gen_fcast.pv_est50_day[0] < 10:
                print("info; generation < 10kW, SoC set to 100%")
            else:

                #  Step through forecast generation and consumption for coming day to identify
                #  lowest minimum before overcharge and maximum overcharge. If there is overcharge
                #  and the first min is above zero, reduce overnight charge for min export.

                batt_charge: float = [0] * 24
                batt_charge[0] = batt_max_charge
                max_charge = 0
                min_charge = batt_max_charge

                print()
                print("{:<20} {:>10} {:>10} {:>10}  {:>10} {:>10}".format("Info; SoC Calcs;",\
                    "Hour", "Charge", "Cons", "Gen", "SoC"))

                index = 0
                while index < 24:
                    if index > 4:  # Battery is in Eco mode
                        total_load = ge.base_load[index]
                    else:  # Battery is in Charge mode
                        total_load = 0
                    est_gen = (gen_fcast.pv_est10_hrly[index] + 2*gen_fcast.pv_est50_hrly[index])/3
                    if index > 0:
                        delta = max(-1 * stgs.GE.charge_rate, \
                            min(stgs.GE.charge_rate, est_gen - total_load))
                        batt_charge[index] = batt_charge[index - 1] + delta
                        # Capture min charge on lowest down-slope before charge exceeds 100%
                        if batt_charge[index] <= batt_charge[index - 1] and\
                            max_charge < batt_max_charge:
                            min_charge = min(min_charge, batt_charge[index])
                        elif index > 4:  # Charging after overnight boost
                            max_charge = max(max_charge, batt_charge[index])
                    soc_pcnt = int(100 * batt_charge[index] / batt_max_charge)
                    print("{:<20} {:>10} {:>10} {:>10}  {:>10} {:>10}".format("Info; SoC Calcs;",\
                        index, round(batt_charge[index], 2),\
                        round(total_load, 2), round(est_gen, 2), soc_pcnt))
                    index += 1

                max_charge_pcnt = int(100 * max_charge / batt_max_charge)
                min_charge_pcnt = int(100 * min_charge / batt_max_charge)

                #  Reduce nightly charge to capture max export
                tgt_soc = max(stgs.GE.min_soc_target, 130 - min_charge_pcnt, 200 - max_charge_pcnt)

                print()
                print("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("Info; SoC Calcs Summary;"\
                    , "Max Charge", "Min Charge", "Max %", "Min %", "Target SoC"))
                print("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("Info; SoC Calcs Summary;"\
                    , round(max_charge, 2), round(min_charge, 2),\
                    max_charge_pcnt, min_charge_pcnt, tgt_soc))
                print("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("Info; SoC (Adjusted);"\
                    , round(max_charge, 2), round(min_charge, 2),\
                    max_charge_pcnt - 100 + tgt_soc, min_charge_pcnt - 100 + tgt_soc, ""))
                print()
        else:
            print("Info; Incomplete Solcast data, setting target SoC to 100%")
        tgt_soc = round(tgt_soc, 0)

        print("Info; SoC Summary; ", LONG_TIME_NOW_VAR, "; Tom Fcast Gen (kWh); ",\
            gen_fcast.pv_est10_day[0], ";", gen_fcast.pv_est50_day[0], ";",\
            gen_fcast.pv_est90_day[0], "; SoC Target (%); ", tgt_soc,\
            "; Today Gen (kWh); ", round(int(PVO_PRT_PAYLOAD["v1"]) / 1000, 2))

        start_time = stgs.GE.start_time
        if tgt_soc < self.soc:  # Delay start to allow partial discharge
            batt_charge_hrs = batt_max_charge / stgs.GE.charge_rate
            charge_time_mins = 60 * batt_charge_hrs * tgt_soc / 100
            discharge_time_mins = 60 * batt_charge_hrs * (self.soc - tgt_soc) / 100
            end_time_mins = time_to_mins(stgs.GE.end_time)
            start_time_mins = time_to_mins(stgs.GE.start_time)
            start_time = time_to_hrs(max(start_time_mins + discharge_time_mins, \
                end_time_mins - charge_time_mins))

        self.set_inverter_register("77", tgt_soc)
        self.set_inverter_register("64", start_time)

# End of GivEnergyObj() class definition

class LoadObj:
    """Class for each controlled load."""

    def __init__(self, load_index: int, l_payload):

        # Pull in data from Load_Config
        self.base_priority: int = load_index + 1  # Sets the initial priority for load
        self.load_record = l_payload  # Pulls in load configuratio details
        self.curr_state: str = "OFF"
        self.prev_state: str = "OFF"
        self.eti: int = 0  # Total minutes on in day
        self.ontime: int = 0  # Minutes since current switch on (+ve) or off (-ve)
        self.priority: int = 99  # Load priority order (0=highest)
        self.priority_change: bool = False  # Used to flag a change in priority
        self.est_power: int = self.load_record["PwrLoad"]
        self.early_start_mins: int = 540
        self.late_start_mins: int = 540
        self.finish_time_mins: int = 1040

        self.parse_sr_ss()

    def parse_sr_ss(self):
        """Substitutes Sunrise / Sunset keywords with actual values."""

        def lookup_time_mins(in_time: str) -> int:
            """Time keyword lookup routine."""
            if in_time == "Sunrise":
                out_time = env_obj.sr_time
            elif in_time == "Sunset":
                out_time = env_obj.ss_time
            elif in_time == "VSunrise":
                out_time = env_obj.virt_sr_time
            elif in_time == "VSunset":
                out_time = env_obj.virt_ss_time
            else:
                out_time = in_time
            out_time_mins = time_to_mins(out_time)
            return out_time_mins

        self.early_start_mins = lookup_time_mins(self.load_record["EarlyStart"])
        self.late_start_mins = lookup_time_mins(self.load_record["LateStart"])
        self.finish_time_mins = lookup_time_mins(self.load_record["FinishTime"])

    def toggle(self, cmd: str) -> float:
        """Command to turn load on or off and return resultant forecast power change."""

        if cmd == "ON" and self.prev_state == "OFF":
            if not TEST_MODE:
                set_mihome_switch(self.load_record["DeviceID"], True)
            print("Device ON event:", self.load_record["DeviceID"], "ETI:", self.eti,
                  "Name:", self.load_record["DeviceName"])
            self.curr_state = "ON"
            self.est_power = self.load_record["PwrLoad"] - self.load_record["Hysteresis"]
            self.eti += 1
            self.ontime = 0  # Reset ontime whenever load toggles state
            return self.est_power

        if cmd == "OFF" and self.prev_state == "ON":
            if not TEST_MODE:
                set_mihome_switch(self.load_record["DeviceID"], False)
            print("Device OFF event:", self.load_record["DeviceID"], "ETI:", self.eti,
                  "Name:", self.load_record["DeviceName"])
            self.curr_state = "OFF"
            self.est_power = self.load_record["PwrLoad"]
            self.ontime = -1  # Start counting down in negative numbers
            return self.est_power

        return 0

    def refresh_priority(self, new_virt_sr_ss: bool):
        """Refresh internal fields and prioritise for load balancing."""

        min_off_time: int = -5  # Prevents load from being turned off and on too quickly

        # Housekeeping first
        if new_virt_sr_ss:
            self.parse_sr_ss()

        self.prev_state = self.curr_state
        if TIME_NOW_MINS_VAR == 0:
            self.eti = 0

        if self.curr_state == "ON":
            self.ontime += 1  # Count up when load is on
            self.eti += 1
        else:
            self.ontime -= 1  # Count down when load is off

        # Does schedule sit within a single day?
        if self.finish_time_mins >= self.early_start_mins:
            valid_time = self.early_start_mins <= TIME_NOW_MINS_VAR < self.finish_time_mins
        else:
            valid_time = self.early_start_mins <= TIME_NOW_MINS_VAR or\
                TIME_NOW_MINS_VAR < self.finish_time_mins

        # Force a start if load has timed-out with run time below its daily target
        late_start_active = self.late_start_mins < TIME_NOW_MINS_VAR and\
            self.eti < self.load_record["MinDailyTarget"]

        # Detect if load has just been turned on and still needs to achieve its minimum on time
        just_on = self.curr_state == "ON" and self.ontime < self.load_record["MinOnTime"]

        # Set priority for load, based on above variables and environmental conditions
        old_priority = self.priority
        if valid_time and (late_start_active or just_on):  # Highest priority: do not turn off
            self.priority = 0
        elif not valid_time or min_off_time < self.ontime < 0:  # Lowest priority: do not turn on
            self.priority = 99
        elif env_obj.co2_intensity > self.load_record['MaxCO2'] or\
            env_obj.temp_deg_c > self.load_record['MaxTemp'] or\
            self.eti >= self.load_record["MaxDailyTarget"] or\
            ge.soc < 50:
            self.priority = 99
        elif self.eti > self.load_record["MinDailyTarget"]:
            self.priority = int(self.base_priority) + 50
        else:
            self.priority = int(self.base_priority)
        self.priority_change = self.priority != old_priority
# End of LoadObj() class definition


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
        solcast_offset = 60 * int(solcast_data_1['forecasts'][0]['period_end'][11:13]) \
            + int(solcast_data_1['forecasts'][0]['period_end'][14:16]) - interval - 60

        index = solcast_offset
        cntr = 0
        while index < forecast_lines * interval:
            pv_est10[index] = int(solcast_data_1['forecasts'][cntr]['pv_estimate10'] * 1000) \
                + int(solcast_data_2['forecasts'][cntr]['pv_estimate10'] * 1000)

            pv_est50[index] = int(solcast_data_1['forecasts'][cntr]['pv_estimate'] * 1000) \
                + int(solcast_data_2['forecasts'][cntr]['pv_estimate'] * 1000)

            pv_est90[index] = int(solcast_data_1['forecasts'][cntr]['pv_estimate90'] * 1000) \
                + int(solcast_data_2['forecasts'][cntr]['pv_estimate90'] * 1000)

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
        print("Info; PV Estimate 10% (hrly, 7 days) / kWh", ";", timestamp, ";",\
            self.pv_est10_hrly[0:23], self.pv_est10_day[0:6])
        print("Info; PV Estimate 50% (hrly, 7 days) / kWh", ";", timestamp, ";",\
            self.pv_est50_hrly[0:23], self.pv_est50_day[0:6])
        print("Info; PV Estimate 90% (hrly, 7 days) / kWh", ";", timestamp, ";",\
            self.pv_est90_hrly[0:23], self.pv_est90_day[0:6])

# End of SolcastObj() class definition

class EnvObj:
    """Stores environmental info - weather, CO2, etc."""

    def __init__(self):
        self.co2_intensity: int = 200
        self.temp_deg_c: float = 15
        self.weather: [str] = []
        self.weather_symbol: str = "0"
        self.current_weather: [str] = []
        self.sunshine: int = 0

        self.sr_time: str = "06:00"
        self.virt_sr_time: str = "09:00"
        self.ss_time: str = "18:00"
        self.virt_ss_time: str = "18:00"

    def update_co2(self):
        """Import latest CO2 intensity data."""

        url = stgs.CarbonIntensity.url + stgs.CarbonIntensity.RegionID

        headers = {
            'Accept': 'application/json'
        }

        try:
            req = requests.get(url, params={}, headers=headers)
            req.raise_for_status()
        except requests.exceptions.RequestException as error:
            print("Warning: Problem obtaining CO2 intensity:", error)
            return

        if len(req.content) < 50:
            print("Warning: Carbon intensity data missing/short")
            #print(req.content)
            return

        co2_intensity_raw: int = []
        co2_intensity_raw = json.loads(req.content.decode('utf-8'))
        self.co2_intensity = co2_intensity_raw['data'][0]['data'][0]['intensity']['forecast']

        if DEBUG_MODE:
            print(co2_intensity_raw)
            print("Info; CO2 Intensity:", self.co2_intensity)


    def reset_sr_ss(self):
        """Reset sunrise & sunset each day."""
        self.sr_time = "06:00"
        self.virt_sr_time = "09:00"
        self.ss_time = "18:00"
        self.virt_ss_time = "18:00"

    def update_weather_curr(self):
        """Download latest weather from OpenWeatherMap."""

        url = stgs.OpenWeatherMap.url + "onecall"
        payload = stgs.OpenWeatherMap.payload

        try:
            req = requests.get(url, params=payload, timeout=5)
            req.raise_for_status()
        except requests.exceptions.RequestException as error:
            print(error)
            return

        if len(req.content) < 50:
            print("Warning: Weather data missing/short")
            print(req.content)
            return

        current_weather = json.loads(req.content.decode('utf-8'))
        #if DEBUG_MODE:
            #pprint(current_weather)
        self.current_weather = current_weather

        self.temp_deg_c = round(current_weather['current']['temp'] - 273, 1)
        self.weather_symbol = current_weather['current']['weather'][0]['id']

# End of EnvObj() class definition

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

def set_mihome_switch(device_id: str, turn_on: bool) -> bool:
    """Operates a MiHome switch on/off."""

    if turn_on:
        sw_cmd = "on"
    else:
        sw_cmd = "off"

    user_id = stgs.MiHome.UserID
    api_key = stgs.MiHome.key
    url = stgs.MiHome.url + "power_" + sw_cmd

    payload = {
        "id" : int(device_id),
    }

    time.sleep(1)  # Hold off for a second to avoid dropped commands at MiHome server

    try:
        req = requests.put(url, auth=(user_id, api_key), json=payload, timeout=5)
        req.raise_for_status()
    except requests.exceptions.RequestException as error:
        print(error)
        return False

    parsed = json.loads(req.content.decode('utf-8'))
    if parsed['status'] == "success":
        return True
    print("Warning; Failure...", url, device_id)
    return False

#  End of set_mihome_switch ()


def put_pv_output():
    """Upload generation/consumption data to PVOutput.org."""

    url = stgs.PVOutput.url + "addstatus.jsp"
    key = stgs.PVOutput.key
    sid = stgs.PVOutput.sid

    post_date = time.strftime("%Y%m%d", time.localtime())
    post_time = time.strftime("%H:%M", time.localtime())

    payload = {
        "t"   : post_time,
        "key" : key,
        "sid" : sid,
        "d"   : post_date
    }

    payload.update(PVO_PRT_PAYLOAD)  # Concatenate the data, don't escape ":"
    payload = urlencode(payload, doseq=True, quote_via=lambda x,y,z,w: x)

    time.sleep(2)  # PVOutput has a 1 second rate limit. Avoid clash with insol download, etc

    if not TEST_MODE and stgs.PVOutput.enable:
        try:
            req = requests.get(url, params=payload, timeout=10)
            req.raise_for_status()
        except requests.exceptions.RequestException as error:
            print("Warning; PVOutput Write Error ", LONG_TIME_NOW_VAR)
            print(error)
            print()
            return()
    print("Data; Write to pvoutput.org;", post_date,";", post_time, ";", PVO_PRT_PAYLOAD)
    return()

#  End of put_pv_output()


def balance_loads():
    """control loads, based on schedule, generation, temp, etc."""

    # Adjust sunrise and sunset to reflect actual conditions
    new_virt_sr_ss = False
    pwr_threshold = stgs.PVData.PwrThreshold
    if TIME_NOW_MINS_VAR < time_to_mins(env_obj.virt_sr_time):  # It's early morning, gen started?
        if ge.sys_status[1]['solar']['power'] < pwr_threshold < ge.sys_status[0]['solar']['power']:
            new_virt_sr_ss = True
            env_obj.virt_sr_time = TIME_NOW_VAR
    elif 900 < TIME_NOW_MINS_VAR < time_to_mins(env_obj.ss_time):  # It's afternoon, gen ended?
        if ge.sys_status[0]['solar']['power'] < pwr_threshold < ge.sys_status[1]['solar']['power']:
            new_virt_sr_ss = True
            env_obj.virt_ss_time = TIME_NOW_VAR
        elif ge.sys_status[0]['solar']['power'] > pwr_threshold:  # False alarm - sun back up
            new_virt_sr_ss = True
            env_obj.virt_ss_time = env_obj.ss_time

    if new_virt_sr_ss and DEBUG_MODE:
        print('Info; VSunrise/set: VSR:', env_obj.virt_sr_time, "VSS:", env_obj.virt_ss_time)

    # Running total of available power. Positive means import
    net_usage_est = 0
    if ge.soc > 95:  # Charge battery first
        net_usage_est = ge.pv_power - ge.consumption

    # First pass: update priority values and make any essential load state changes
    for unique_load in load_obj:
        unique_load.refresh_priority(new_virt_sr_ss)
        if unique_load.priority_change:
            if unique_load.priority == 99:
                net_usage_est -= unique_load.toggle("OFF")
            elif unique_load.priority == 0:
                net_usage_est += unique_load.toggle("ON")
        elif 51 <= unique_load.priority <= 90 and\
            unique_load.curr_state == "ON":  # Active low-priority loads are up for grabs
            net_usage_est -= unique_load.est_power

    # Second pass: if possible, turn on new loads, highest priority first
    for priority in range(1, 90):
        for unique_load in load_obj:
            if net_usage_est < 0 and\
                unique_load.priority == priority and\
                net_usage_est * -1 >= unique_load.est_power and\
                unique_load.curr_state == "OFF":  # Capacity exists, turn on load
                net_usage_est += unique_load.toggle("ON")

    # Third pass: Turn off loads to rebalance power, lowest priority first
    for priority in range(90, 1, -1):
        for unique_load in load_obj:
            if net_usage_est > 0 and\
                unique_load.priority == priority and\
                unique_load.curr_state == "ON":  # Turn off load
                net_usage_est -= unique_load.toggle("OFF")

#  End of balance_loads()


if __name__ == '__main__':

    # Current time definitions
    LONG_TIME_NOW_VAR: str = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime())
    TIME_NOW_VAR: str = LONG_TIME_NOW_VAR[11:]
    TIME_NOW_MINS_VAR: int = time_to_mins(TIME_NOW_VAR)

    print("Info; PALM... PV Automated Load Manager Version:", PALM_VERSION)
    print("Info; Initialising at", LONG_TIME_NOW_VAR)

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

    # Read in load configuration file

    LOAD_CONFIG = stgs.LOAD_CONFIG

    # Declare Power objects
    ge: GivEnergyObj = GivEnergyObj()
    ge.get_load_hist()

    # Predict PV output
    #insol: InsolationObj = InsolationObj()
    #insol.update_insol()
    solcast: SolcastObj = SolcastObj()
    solcast.update()

    # Misc environmental data: weather, CO2, etc
    CO2_USAGE_VAR: int = 200
    env_obj: EnvObj = EnvObj()
    env_obj.update_weather_curr()
    env_obj.update_co2()

    # Create an object for each load
    load_obj: List[LoadObj] = []
    load_payload: List[str] = []
    NUM_LOADS: int = len(LOAD_CONFIG['LoadPriorityOrder'])
    for LOAD_INDEX_VAR in range(NUM_LOADS):
        load_payload = LOAD_CONFIG[LOAD_CONFIG['LoadPriorityOrder'][LOAD_INDEX_VAR]]
        load_obj.append(LoadObj(LOAD_INDEX_VAR, load_payload))

    print("Info; Entering main loop...")
    sys.stdout.flush()

    LOOP_COUNTER_VAR: int = 0  # 1 minute minor frame
    PVO_TSTAMP_VAR: int = 0  # Records when PV data last writtem

    while True:  # Main Loop

        # Sync to minute rollover on system clock
        CURRENT_MINUTE = int(time.strftime("%M", time.localtime()))

        if not DEBUG_MODE:
            while int(time.strftime("%M", time.localtime())) == CURRENT_MINUTE:
                time.sleep(1)
        else:
            if LOOP_COUNTER_VAR == 0:
                print("Debug mode... 5 second delay")
            time.sleep(5)

        # Set parameters for this frame
        LONG_TIME_NOW_VAR = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime())
        TIME_NOW_VAR = LONG_TIME_NOW_VAR[11:]
        TIME_NOW_MINS_VAR = time_to_mins(TIME_NOW_VAR)

        # Run activities, depending on specific intervals
        if TIME_NOW_MINS_VAR == 1435:  # 5 to midnight for next days forecasts
            #do_get_insolation = threading.Thread(target=insol.update_insol())
            #do_get_insolation.daemon = True
            #do_get_insolation.start()

            do_get_solcast = threading.Thread(target=solcast.update())
            do_get_solcast.daemon = True
            do_get_solcast.start()

        if (DEBUG_MODE and LOOP_COUNTER_VAR == 2) or TIME_NOW_MINS_VAR == 1438:
            # Set SoC target
            ge.get_load_hist()
            ge.compute_tgt_soc(solcast)
            # Reset sunrise and sunset fro next day
            env_obj.reset_sr_ss()

        if LOOP_COUNTER_VAR % 15 == 14:  # Update carbon intensity every 15 mins
            do_get_carbon_intensity = threading.Thread(target=env_obj.update_co2())
            do_get_carbon_intensity.daemon = True
            do_get_carbon_intensity.start()

        if LOOP_COUNTER_VAR % 15 == 14:  # Update weather every 10 mins
            do_get_weather = threading.Thread(target=env_obj.update_weather_curr())
            do_get_weather.daemon = True
            do_get_weather.start()

        #  Refresh utilisation data from GivEnergy server
        ge.get_latest_data()

        CO2_USAGE_VAR = int(env_obj.co2_intensity * ge.grid_power / 1000)

        do_balance_loads = threading.Thread(target=balance_loads())
        do_balance_loads.daemon = True
        do_balance_loads.start()

        # Publish data to PVOutput.org every 5 minutes (or 5 cycles as a catch-all)
        if DEBUG_MODE or TIME_NOW_MINS_VAR % 5 == 3 or LOOP_COUNTER_VAR > PVO_TSTAMP_VAR + 4:

            PVO_TSTAMP_VAR = LOOP_COUNTER_VAR

            batt_power_out = ge.batt_power if ge.batt_power > 0 else 0
            batt_power_in = 0 if ge.batt_power > 0 else ge.batt_power * -1
            total_cons = ge.consumption - ge.batt_power
            load_pwr = total_cons if total_cons > 0 else 0

            PVO_PRT_PAYLOAD = {
                "v1"  : ge.pv_energy,
                "v2"  : ge.pv_power,
                "v3"  : ge.grid_energy,
                "v4"  : load_pwr,
                "v5"  : env_obj.temp_deg_c,
                "v6"  : ge.line_voltage,
                "v7"  : "",
                "v8"  : batt_power_out,
                "v9"  : env_obj.co2_intensity,
                "v10" : CO2_USAGE_VAR,
                "v11" : batt_power_in,
                "v12" : ge.soc,
            }

            do_put_pv_output = threading.Thread(target=put_pv_output)
            do_put_pv_output.daemon = True
            do_put_pv_output.start()

        sys.stdout.flush()

        LOOP_COUNTER_VAR += 1
        if TIME_NOW_MINS_VAR == 0:  # Reset frame counter every 24 hours
            LOOP_COUNTER_VAR = 1

# End of main
