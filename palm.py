#!/usr/bin/env python3
"""PALM - PV Active Load Manager."""

import sys
import time
import threading
import json
from datetime import datetime, timedelta
from typing import Tuple, List
from urllib.parse import urlencode
import logging
import random
## import matplotlib.pyplot as plt
import requests
import settings as stgs

# This software in any form is covered by the following Open Source BSD license:
#
# Copyright 2023, Steve Lewis
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
# 2. Load management - lights, excess power dumping, etc
# 3. Setting overnight charge point, based on SolCast forecast & actual usage
###########################################

# Changelog:
# v0.6.0    12/Feb/22 First cut at GivEnergy interface
# ...
# v0.10.0   21/Jun/23 Added multi-day averaging for usage calcs
# v1.0.0    15/Jul/23 Random start time, Solcast data correction, IO compatibility, 48-hour fcast

# NOTE: To enable plot capability uncomment all lines begiinging with "##"

PALM_VERSION = "v1.0.0"
# -*- coding: utf-8 -*-
# pylint: disable=logging-not-lazy
# pylint: disable=consider-using-f-string

class GivEnergyObj:
    """Class for GivEnergy inverter"""

    def __init__(self):
        sys_item = {'time': '',
                    'solar': {'power': 0, 'arrays':
                                  [{'array': 1, 'voltage': 0, 'current': 0, 'power': 0},
                                   {'array': 2, 'voltage': 0, 'current': 0, 'power': 0}]},
                    'grid': {'voltage': 0, 'current': 0, 'power': 0, 'frequency': 0},
                    'battery': {'percent': 0, 'power': 0, 'temperature': 0},
                    'inverter': {'temperature': 0, 'power': 0, 'output_voltage': 0, \
                        'output_frequency': 0, 'eps_power': 0},
                    'consumption': 0}
        self.sys_status: List[str] = [sys_item] * 5

        meter_item = {'time': '',
                      'today': {'solar': 0, 'grid': {'import': 0, 'export': 0},
                                'battery': {'charge': 0, 'discharge': 0}, 'consumption': 0},
                      'total': {'solar': 0, 'grid': {'import': 0, 'export': 0},
                                'battery': {'charge': 0, 'discharge': 0}, 'consumption': 0}}
        self.meter_status: List[str] = [meter_item] * 5

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
        self.tgt_soc = 100
        self.cmd_list = stgs.GE_Command_list['data']

        logger.debug("Valid inverter commands:")
        for line in self.cmd_list:
            logger.debug(str(line['id'])+ "- "+ str(line['name']))

    def get_latest_data(self):
        """Download latest data from GivEnergy."""

        utc_timenow_mins = t_to_mins(time.strftime("%H:%M:%S", time.gmtime()))
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
                logger.error(error)
                return

            if len(resp.content) > 100:
                for i in range(4, -1, -1):  # Right shift old data
                    if i > 0:
                        self.sys_status[i] = self.sys_status[i - 1]
                    else:
                        try:
                            self.sys_status[i] = \
                                json.loads(resp.content.decode('utf-8'))['data']
                        except Exception:
                            logger.error("Error reading GivEnergy system status "+ T_NOW_VAR)
                            logger.error(resp.content)
                            self.sys_status[i] = self.sys_status[i + 1]
                if LOOP_COUNTER_VAR == 0:  # Pack array on startup
                    i = 1
                    while i < 5:
                        self.sys_status[i] = self.sys_status[0]
                        i += 1
                self.read_time_mins = t_to_mins(self.sys_status[0]['time'][11:])
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
                logger.error(error)
                return

            if len(resp.content) > 100:
                for i in range(4, -1, -1):  # Right shift old data
                    if i > 0:
                        self.meter_status[i] = self.meter_status[i - 1]
                    else:
                        try:
                            self.meter_status[i] = \
                                json.loads(resp.content.decode('utf-8'))['data']
                        except Exception:
                            logger.error("Error reading GivEnergy meter status "+ T_NOW_VAR)
                            logger.error(resp.content)
                            self.meter_status[i] = self.meter_status[i + 1]
                if LOOP_COUNTER_VAR == 0:  # Pack array on startup
                    i = 1
                    while i < 5:
                        self.meter_status[i] = self.meter_status[0]
                        i += 1

                self.pv_energy = int(self.meter_status[0]['today']['solar'] * 1000)

                # Daily grid energy must be >=0 for PVOutput.org (battery charge >= midnight value)
                self.grid_energy = max(int(self.meter_status[0]['today']['consumption'] * 1000), 0)

    def get_load_hist(self):
        """Download historical consumption data from GivEnergy and pack array for next SoC calc"""

        def get_load_hist_day(offset: int):
            """Get load history for a single day"""

            load_array = [0] * 48
            day_delta = offset if (T_NOW_MINS_VAR > 1260) else offset + 1  # Use latest day if >9pm
            day = datetime.strftime(datetime.now() - timedelta(day_delta), '%Y-%m-%d')
            url = stgs.GE.url + "data-points/"+ day
            key = stgs.GE.key
            headers = {
                'Authorization': 'Bearer  ' + key,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            params = {
                'page': '1',
                'pageSize': '2000'
            }

            try:
                resp = requests.request('GET', url, headers=headers, params=params)
            except requests.exceptions.RequestException as error:
                logger.error(error)
                return load_array
            if resp.status_code != 200:
                logger.error("Invalid response: "+ str(resp.status_code))
                return load_array

            if len(resp.content) > 100:
                history = json.loads(resp.content.decode('utf-8'))
                i = 6
                counter = 0
                current_energy = prev_energy = 0
                while i < 290:
                    try:
                        current_energy = float(history['data'][i]['today']['consumption'])
                    except Exception:
                        break
                    if counter == 0:
                        load_array[counter] = round(current_energy, 1)
                    else:
                        load_array[counter] = round(current_energy - prev_energy, 1)
                    counter += 1
                    prev_energy = current_energy
                    i += 6
            return load_array

        load_hist_array = [0] * 48
        acc_load = [0] * 48
        total_weight: int = 0

        i: int = 0
        while i < len(stgs.GE.load_hist_weight):
            if stgs.GE.load_hist_weight[i] > 0:
                logger.info("Processing load history for day -"+ str(i + 1))
                load_hist_array = get_load_hist_day(i)
                j = 0
                while j < 48:
                    acc_load[j] += load_hist_array[j] * stgs.GE.load_hist_weight[i]
                    acc_load[j] = round(acc_load[j], 2)
                    j += 1
                total_weight += stgs.GE.load_hist_weight[i]
                logger.debug(str(acc_load)+ " total weight: "+ str(total_weight))
            else:
                logger.info("Skipping load history for day -"+ str(i + 1)+ " (weight = 0)")
            i += 1

        # Calculate averages and write results
        i = 0
        while i < 48:
            self.base_load[i] = round(acc_load[i]/total_weight, 1)
            i += 1

        logger.info("Load Calc Summary: "+ str(self.base_load))

    def set_mode(self, cmd: str, *arg: str):
        """Configures inverter operating mode"""

        def set_inverter_register(register: str, value: str):
            """Exactly as it says"""

            # Validate command against list in settings
            cmd_name = ""
            valid_cmd = False
            for line in self.cmd_list:
                if line['id'] == int(register):
                    cmd_name = line['name']
                    valid_cmd = True
                    break

            if valid_cmd is False:
                logger.critical("write attempt to invalid inverter register: "+ str(register))
                return

            url = stgs.GE.url + "settings/"+ register + "/write"
            key = stgs.GE.key
            headers = {
                'Authorization': 'Bearer  ' + key,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            payload = {
                'value': value
            }
            resp = "TEST"
            if not TEST_MODE:
                try:
                    resp = requests.request('POST', url, headers=headers, json=payload)
                except requests.exceptions.RequestException as error:
                    logger.error(error)
                    return
                if resp.status_code != 201:
                    logger.info("Invalid response: "+ str(resp.status_code))
                    return

            logger.info("Setting Register "+ str(register)+ " ("+ str(cmd_name) + ") to "+
                        str(value)+ "   Response: "+ str(resp))

            time.sleep(3)  # Allow data on GE servver to settle

            # Readback check
            url = stgs.GE.url + "settings/"+ register + "/read"
            key = stgs.GE.key
            headers = {
                'Authorization': 'Bearer  ' + key,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            payload = {}

            try:
                resp = requests.request('POST', url, headers=headers, json=payload)
            except resp.exceptions.RequestException as error:
                logger.error(error)
                return
            if resp.status_code != 201:
                logger.error("Invalid response: "+ str(resp.status_code))
                return

            returned_cmd = json.loads(resp.content.decode('utf-8'))['data']['value']
            if str(returned_cmd) == str(value):
                logger.info("Successful register read: "+ str(register)+ " = "+ str(returned_cmd))
            else:
                logger.error("Readback failed on GivEnergy API... Expected " +
                    str(value) + ", Read: "+ str(returned_cmd))


        if cmd == "set_soc":  # Sets target SoC to value, randomises start time to be grid friendly
            set_inverter_register("77", arg[0])
            if stgs.GE.start_time != "":
                start_time = t_to_hrs(t_to_mins(stgs.GE.start_time) + random.randint(1,14))
                set_inverter_register("64", start_time)
            if stgs.GE.end_time != "":
                set_inverter_register("65", stgs.GE.end_time)

        elif cmd == "set_soc_winter":  # Restore default overnight charge params
            set_inverter_register("77", "100")
            if stgs.GE.start_time != "":
                start_time = t_to_hrs(t_to_mins(stgs.GE.start_time) + random.randint(1,14))
                set_inverter_register("64", stgs.GE.start_time)
            if stgs.GE.end_time_winter != "":
                set_inverter_register("65", stgs.GE.end_time_winter)

        elif cmd == "charge_now":
            set_inverter_register("77", "100")
            set_inverter_register("64", "00:01")
            set_inverter_register("65", "23:59")

        elif cmd == "pause":
            set_inverter_register("72", "0")
            set_inverter_register("73", "0")

        elif cmd == "resume":
            set_inverter_register("72", "3000")
            set_inverter_register("73", "3000")

        else:
            logger.error("unknown inverter command: "+ cmd)

    def compute_tgt_soc(self, gen_fcast, weight: int, commit: bool):
        """Compute overnight SoC target"""

        # Winter months = 100%
        if MNTH_VAR in stgs.GE.winter and commit:  # No need for sums...
            logger.info("winter month, SoC set to 100")
            self.set_mode("set_soc_winter")
            return

        # Quick check for valid generation data
        if gen_fcast.pv_est50_day[0] == 0:
            logger.error("Missing generation data, SoC set to 100")
            self.set_mode("set_soc")
            return

        # Solcast provides 3 estimates (P10, P50 and P90). Compute individual weighting
        # factors for each of the 3 estimates from the weight input parameter, using a
        # triangular approximation for simplicity

        weight = min(max(weight,10),90)  # Range check
        wgt_10 = max(0, 50 - weight)
        if weight > 50:
            wgt_50 = 90 - weight
        else:
            wgt_50 = weight - 10
        wgt_90 = max(0, weight - 50)

        logger.info("")
        logger.info("{:<20} {:>10} {:>10} {:>10} {:>10}  {:>10} {:>10}".format("SoC Calc;",
            "Day", "Hour", "Charge", "Cons", "Gen", "SoC"))
##        plot_x = []
##        plot_y1 = []
##        plot_y2 = []
##        plot_y3 = []
##        plot_y4 = []

        if stgs.GE.end_time != "":
            end_charge_period = int(stgs.GE.end_time[0:2]) * 2
        else:
            end_charge_period = 8

        batt_max_charge: float = stgs.GE.batt_max_charge
        batt_charge: float = [0] * 98
        reserve_energy = batt_max_charge * stgs.GE.batt_reserve / 100
        max_charge_pcnt = [0] * 2
        min_charge_pcnt = [0] * 2

        # The clever bit:
        # Start with battery at reserve %. For each 30-minute slot of the coming day, calculate
        # the battery charge based on forecast generation and historical usage. Capture values
        # for maximum charge and also the minimum charge value at any time before the maximum.

        day = 0
        while day < 2:  # Repeat for tomorrow and next day
            batt_charge[0] = max_charge = min_charge = reserve_energy
            est_gen = 0
            i = 0
            while i < 48:
                if i <= end_charge_period:  # Battery is in AC Charge mode
                    total_load = 0
                    batt_charge[i] = batt_charge[0]
                else:
                    total_load = ge.base_load[i]
                    est_gen = (gen_fcast.pv_est10_30[day*48 + i] * wgt_10 +
                        gen_fcast.pv_est50_30[day*48 + i] * wgt_50 +
                        gen_fcast.pv_est90_30[day*48 + i] * wgt_90) / (wgt_10 + wgt_50 + wgt_90)
                    batt_charge[i] = (batt_charge[i - 1] +
                        max(-1 * stgs.GE.charge_rate,
                        min(stgs.GE.charge_rate, (est_gen - total_load))))

                # Capture min charge on lowest point on down-slope before charge reaches 100%
                # or max charge if on an up slope after overnight charge
                if (batt_charge[i] <= batt_charge[i - 1] and
                    max_charge < batt_max_charge):
                    min_charge = min(min_charge, batt_charge[i])
                elif i > end_charge_period:  # Charging after overnight boost
                    max_charge = max(max_charge, batt_charge[i])

                logger.info("{:<20} {:>10} {:>10} {:>10} {:>10}  {:>10} {:>10}".format("SoC Calc;",
                    day, t_to_hrs(i * 30), round(batt_charge[i], 2), round(total_load, 2),
                    round(est_gen, 2), int(100 * batt_charge[i] / batt_max_charge)))
##                plot_x.append(t_to_hrs((day*48 + i) * 30))
##                plot_y1.append(int(100 * batt_charge[i])/batt_max_charge)
##                plot_y3.append(100)
##                plot_y4.append(stgs.GE.batt_reserve)

                i += 1

            max_charge_pcnt[day] = int(100 * max_charge / batt_max_charge)
            min_charge_pcnt[day] = int(100 * min_charge / batt_max_charge)

            day += 1

        # low_soc is the minimum SoC target. Provide more buffer capacity in shoulder months
        # when load is likely to be more variable, e.g. heating
        if MNTH_VAR in stgs.GE.shoulder:
            low_soc = stgs.GE.max_soc_target
        else:
            low_soc = stgs.GE.min_soc_target

        # So we now have the four values of max & min charge for tomorrow & overmorrow
        # Check if overmorrow is better than tomorrow and there is opportunity to reduce target
        # to avoid residual charge at the end of the day in anticipation of a sunny day
        if max_charge_pcnt[1] > 100 - low_soc > max_charge_pcnt[0]:
            logger.info("Overmorrow correction applied")
            max_charge_pc = max_charge_pcnt[0] + (max_charge_pcnt[1] - 100) / 2
        else:
            logger.info("Overmorrow correction not needed/applied")
            max_charge_pc = max_charge_pcnt[0]
        min_charge_pc = min_charge_pcnt[0]

        print("Min & max", min_charge_pc, max_charge_pc)
        # The really clever bit: reduce the target SoC to the greater of:
        #     The surplus above 100% for max_charge_pcnt
        #     The value needed to achieve the stated spare capacity at minimum charge point
        #     The preset minimum value
        tgt_soc = max(100 - max_charge_pc, (low_soc - min_charge_pc), low_soc)
        # Range check the resulting value
        tgt_soc = int(min(tgt_soc, 100))  # Limit range to 100%

        # Produce SoC plots (y1 = baseline, y2 = adjusted)
##        day = 0
##        diff = tgt_soc
##        while day < 2:
##            i = 0
##            while i < 48:
##                if day == 1 and i == 0:
##                    diff = plot_y2[47] - plot_y1[48]
##                if plot_y1[day*48 + i] + diff > 100:  # Correct for SoC > 100%
##                    diff = 100 - plot_y1[day*48 + i]
##                plot_y2.append(plot_y1[day*48 + i] + diff)
##                i += 1
##            day += 1

        logger.info("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("SoC Calc Summary;",
            "Max Charge", "Min Charge", "Max %", "Min %", "Target SoC"))
        logger.info("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("SoC Calc Summary;",
            round(max_charge, 2), round(min_charge, 2),
            max_charge_pc, min_charge_pc, tgt_soc))
        logger.info("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("SoC (Adjusted);",
            round(max_charge, 2), round(min_charge, 2),
            max_charge_pc + tgt_soc, min_charge_pc + tgt_soc, "\n"))

##        plt.plot(plot_x, plot_y1, label='Baseline SoC', color='blue')
##        plt.plot(plot_x, plot_y2, label='Adjusted SoC', color='black')
##        plt.plot(plot_x, plot_y3, label='Capacity', color='red', linestyle='dashed')
##        plt.plot(plot_x, plot_y4, label='Reserve', color='orange', linestyle='dashed')
##        plt.xlabel('Time')
##        plt.ylabel('SoC (%)')
##        plt.title('SoC Summary '+ str(weight)+ '%')
##        plt.tick_params(axis='x', rotation=55)
##        plt.legend()
##        if TEST_MODE is True:
##            plt.show()
##        else:
##            plt.savefig('palm_plot.png')
##            logger.info("Saving SoC summary plot as palm_plot.png")

        if commit:
            logger.critical("Sending calculated SoC to inverter: "+ str(tgt_soc))
            self.set_mode("set_soc", str(tgt_soc))
            self.tgt_soc = tgt_soc

# End of GivEnergyObj() class definition

class LoadObj:
    """Class for each controlled load."""

    def __init__(self, load_i: int, l_payload):

        # Pull in data from Load_Config
        self.base_priority: int = load_i + 1  # Sets the initial priority for load
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
            out_time_mins = t_to_mins(out_time)
            return out_time_mins

        self.early_start_mins = lookup_time_mins(self.load_record["EarlyStart"])
        self.late_start_mins = lookup_time_mins(self.load_record["LateStart"])
        self.finish_time_mins = lookup_time_mins(self.load_record["FinishTime"])

    def toggle(self, cmd: str) -> float:
        """Command to turn load on or off and return resultant forecast power change."""

        if cmd == "ON" and self.prev_state == "OFF":
            if not TEST_MODE:
                set_mihome_switch(self.load_record["DeviceID"], True)
            logger.info("Device ON event: "+ str(self.load_record["DeviceID"])+ " ETI: "+
                        str(self.eti)+ " Name: "+ str(self.load_record["DeviceName"]))
            self.curr_state = "ON"
            self.est_power = self.load_record["PwrLoad"] - self.load_record["Hysteresis"]
            self.eti += 1
            self.ontime = 0  # Reset ontime whenever load toggles state
            return self.est_power

        if cmd == "OFF" and self.prev_state == "ON":
            if not TEST_MODE:
                set_mihome_switch(self.load_record["DeviceID"], False)
            logger.info("Device OFF event: "+ str(self.load_record["DeviceID"])+ " ETI: "+
                        str(self.eti)+ " Name: "+ str(self.load_record["DeviceName"]))
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
        if T_NOW_MINS_VAR == 0:
            self.eti = 0

        if self.curr_state == "ON":
            self.ontime += 1  # Count up when load is on
            self.eti += 1
        else:
            self.ontime -= 1  # Count down when load is off

        # Does schedule sit within a single day?
        if self.finish_time_mins >= self.early_start_mins:
            valid_time = self.early_start_mins <= T_NOW_MINS_VAR < self.finish_time_mins
        else:
            valid_time = (self.early_start_mins <= T_NOW_MINS_VAR or
                T_NOW_MINS_VAR < self.finish_time_mins)

        # Force a start if load has timed-out with run time below its daily target
        late_start_active = (self.late_start_mins < T_NOW_MINS_VAR and
            self.eti < self.load_record["MinDailyTarget"])

        # Detect if load has just been turned on and still needs to achieve its minimum on time
        just_on = self.curr_state == "ON" and self.ontime < self.load_record["MinOnTime"]

        # Set priority for load, based on above variables and environmental conditions
        old_priority = self.priority
        if valid_time and (late_start_active or just_on):  # Highest priority: do not turn off
            self.priority = 0
        elif not valid_time or min_off_time < self.ontime < 0:  # Lowest priority: do not turn on
            self.priority = 99
        elif (env_obj.co2_intensity > self.load_record['MaxCO2'] or
            env_obj.temp_deg_c > self.load_record['MaxTemp'] or
            self.eti >= self.load_record["MaxDailyTarget"] or
            ge.soc < 50):
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
        self.pv_est50_day: [int] = [0] * 7
        self.pv_est90_day: [int] = [0] * 7

        self.pv_est10_30: [int] = [0] * 96
        self.pv_est50_30: [int] = [0] * 96
        self.pv_est90_30: [int] = [0] * 96

    def update(self):
        """Updates forecast generation from Solcast."""

        def get_solcast(url) -> Tuple[bool, str]:
            """Download latest Solcast forecast."""

            solcast_url = url + stgs.Solcast.cmd + "&api_key="+ stgs.Solcast.key
            try:
                resp = requests.get(solcast_url, timeout=5)
                resp.raise_for_status()
            except requests.exceptions.RequestException as error:
                logger.error(error)
                return False, ""
            if resp.status_code != 200:
                logger.error("Invalid response: "+ str(resp.status_code))
                return False, ""

            if len(resp.content) < 50:
                logger.warning("Warning: Solcast data missing/short")
                logger.warning(resp.content)
                return False, ""

            solcast_data = json.loads(resp.content.decode('utf-8'))
            logger.debug(str(solcast_data))

            return True, solcast_data
        #  End of get_solcast()

        # Download latest data for each array, abort if unsuccessful
        result, solcast_data_1 = get_solcast(stgs.Solcast.url_se)
        if not result:
            logger.warning("Error; Problem with Solcast data, using previous values (if any)")
            return

        if stgs.Solcast.url_sw != "":  # Two arrays are specified
            logger.info("url_sw = '"+str(stgs.Solcast.url_sw)+"'")
            result, solcast_data_2 = get_solcast(stgs.Solcast.url_sw)
            if not result:
                logger.warning("Error; Problem with Solcast data, using previous values (if any)")
                return
        else:
            logger.info("No second array")

        logger.info("Successful Solcast download.")

        # Combine forecast for PV arrays & align data with day boundaries
        pv_est10 = [0] * 10080
        pv_est50 = [0] * 10080
        pv_est90 = [0] * 10080

        if stgs.Solcast.url_sw != "":  # Two arrays are specified
            forecast_lines = min(len(solcast_data_1['forecasts']), len(solcast_data_2['forecasts'])) - 1
        else:
            forecast_lines = len(solcast_data_1['forecasts']) - 1
        interval = int(solcast_data_1['forecasts'][0]['period'][2:4])
        solcast_offset = t_to_mins(solcast_data_1['forecasts'][0]['period_end'][11:16]) - interval - 60

        # Check for BST and convert to local time to align with GivEnergy data
        if time.strftime("%z", time.localtime()) == "+0100":
            logger.info("Applying BST offset to Solcast data")
            solcast_offset += 60

        i = solcast_offset
        cntr = 0
        while i < solcast_offset + forecast_lines * interval:
            try:
                if stgs.Solcast.url_sw != "":  # Two arrays are specified
                    pv_est10[i] = (int(solcast_data_1['forecasts'][cntr]['pv_estimate10'] * 1000) +
                        int(solcast_data_2['forecasts'][cntr]['pv_estimate10'] * 1000))
                    pv_est50[i] = (int(solcast_data_1['forecasts'][cntr]['pv_estimate'] * 1000) +
                        int(solcast_data_2['forecasts'][cntr]['pv_estimate'] * 1000))
                    pv_est90[i] = (int(solcast_data_1['forecasts'][cntr]['pv_estimate90'] * 1000) +
                        int(solcast_data_2['forecasts'][cntr]['pv_estimate90'] * 1000))
                else:
                    pv_est10[i] = int(solcast_data_1['forecasts'][cntr]['pv_estimate10'] * 1000)
                    pv_est50[i] = int(solcast_data_1['forecasts'][cntr]['pv_estimate'] * 1000)
                    pv_est90[i] = int(solcast_data_1['forecasts'][cntr]['pv_estimate90'] * 1000)
            except Exception:
                logger.error("Error: Unexpected end of Solcast data. i="+ str(i)+ "cntr="+ str(cntr))
                break
            
            if i > 1 and i % interval == 0:
                cntr += 1
            i += 1

        if solcast_offset > 720:  # Forget about current day as it's already afternoon
            offset = 1440 - 90
        else:
            offset = 0

        i = 0
        while i < 7:  # Summarise daily forecasts
            start = i * 1440 + offset + 1
            end = start + 1439
            self.pv_est10_day[i] = round(sum(pv_est10[start:end]) / 60000, 3)
            self.pv_est50_day[i] = round(sum(pv_est50[start:end]) / 60000, 3)
            self.pv_est90_day[i] = round(sum(pv_est90[start:end]) / 60000, 3)
            i += 1

        i = 0
        while i < 96:  # Calculate half-hourly generation
            start = i * 30 + offset + 1
            end = start + 29
            self.pv_est10_30[i] = round(sum(pv_est10[start:end])/60000, 3)
            self.pv_est50_30[i] = round(sum(pv_est50[start:end])/60000, 3)
            self.pv_est90_30[i] = round(sum(pv_est90[start:end])/60000, 3)
            i += 1

        timestamp = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime())
        logger.info("PV Estimate 10% (hrly, 7 days) / kWh; "+ timestamp+ "; "+
            str(self.pv_est10_30[0:47])+ str(self.pv_est10_day[0:6]))
        logger.info("PV Estimate 50% (hrly, 7 days) / kWh; "+ timestamp+ "; "+
            str(self.pv_est50_30[0:47])+ str(self.pv_est50_day[0:6]))
        logger.info("PV Estimate 90% (hrly, 7 days) / kWh; "+ timestamp+ "; "+
            str(self.pv_est90_30[0:47])+ str(self.pv_est90_day[0:6]))

# End of SolcastObj() class definition

class EnvObj:
    """Stores environmental info - weather, CO2, etc."""

    def __init__(self):
        self.co2_intensity: int = 200
        self.co2_high: bool = False
        self.temp_deg_c: float = 15
        self.weather: [str] = []
        self.weather_symbol: str = "0"
        self.current_weather: [str] = []
        self.sunshine: int = 0
        self.sr_time: str = "06:00"
        self.virt_sr_time: str = "09:00"
        self.ss_time: str = "21:00"
        self.virt_ss_time: str = "21:00"

    def update_co2(self):
        """Import latest CO2 intensity data."""

        timestring = time.strftime("%Y-%m-%dT%H:%MZ", time.localtime())
        url = stgs.CarbonIntensity.url + timestring + stgs.CarbonIntensity.RegionID

        headers = {
            'Accept': 'application/json'
        }

        try:
            resp = requests.get(url, params={}, headers=headers)
            resp.raise_for_status()
        except requests.exceptions.RequestException as error:
            logger.warning("Warning: Problem obtaining CO2 intensity: "+ str(error))
            return

        if len(resp.content) < 50:
            logger.warning("Warning: Carbon intensity data missing/short")
            return

        co2_intens_raw: int = []
        co2_intens_raw = json.loads(resp.content.decode('utf-8'))['data']['data']

        self.co2_intensity = co2_intens_raw[0]['intensity']['forecast']

        co2_intens_near = 0
        co2_intens_far = 0
        i = 0
        try:
            while i < 5:
                co2_intens_near += int(co2_intens_raw[i]['intensity']['forecast']) / 5
                co2_intens_far += int(co2_intens_raw[i + 6]['intensity']['forecast']) / 5
                i += 1
            co2_intens_near = round(co2_intens_near, 0)
            co2_intens_far = round(co2_intens_far, 0)

        except Exception as error:
            logger.warning("Warning: Problem calculating CO2 intensity trend: "+ str(error))

        self.co2_high = co2_intens_far > 1.3 * co2_intens_near or \
            co2_intens_far > stgs.CarbonIntensity.Threshold and \
            co2_intens_far > co2_intens_near

        logger.debug(str(co2_intens_raw))
        logger.debug("CO2 Intensity: "+ str(self.co2_intensity)+ str(co2_intens_near)+
            str(co2_intens_far)+ str(self.co2_high))

    def check_sr_ss(self) -> bool:
        """Adjust sunrise and sunset to reflect actual conditions"""

        new_virt_sr_ss = False
        pwr_threshold = stgs.PVData.PwrThreshold
        if T_NOW_MINS_VAR < t_to_mins(env_obj.virt_sr_time):  # Gen started?
            if (ge.sys_status[1]['solar']['power'] < pwr_threshold <
                ge.sys_status[0]['solar']['power']):
                new_virt_sr_ss = True
                self.virt_sr_time = ge.sys_status[0]['time'][11:]
                logger.info("VSunrise/set (Sunrise detected) VSR: " +
                      str(env_obj.virt_sr_time)+ " VSS: "+ str(env_obj.virt_ss_time))
        elif T_NOW_MINS_VAR > 900:  # It's afternoon, gen ended?
            if (ge.sys_status[0]['solar']['power'] < pwr_threshold and
                (pwr_threshold < ge.sys_status[1]['solar']['power'] or LOOP_COUNTER_VAR < 10)):
                new_virt_sr_ss = True
                self.virt_ss_time = ge.sys_status[0]['time'][11:]
                logger.info("VSunrise/set (Sunset detected) VSR: " +
                      str(env_obj.virt_sr_time)+ " VSS: "+ str(env_obj.virt_ss_time))
            elif (ge.sys_status[0]['solar']['power'] > 2 * pwr_threshold >
                ge.sys_status[1]['solar']['power']):
                # False alarm - sun back up (added hyteresis to threshold)
                new_virt_sr_ss = True
                self.virt_ss_time = env_obj.ss_time
                logger.info('VSunrise/set (False alarm) VSR:' +
                      str(env_obj.virt_sr_time)+ " VSS:"+ str(env_obj.virt_ss_time))
        return new_virt_sr_ss

    def reset_sr_ss(self):
        """Reset sunrise & sunset each day."""

        self.sr_time: str = "06:00"
        self.virt_sr_time: str = "09:00"
        self.ss_time: str = "21:30"
        self.virt_ss_time: str = "21:30"

    def update_weather_curr(self):
        """Download latest weather from OpenWeatherMap."""

        url = stgs.OpenWeatherMap.url + "onecall"
        payload = stgs.OpenWeatherMap.payload

        try:
            resp = requests.get(url, params=payload, timeout=5)
            resp.raise_for_status()
        except requests.exceptions.RequestException as error:
            logger.error(error)
            return

        if len(resp.content) < 50:
            logger.warning("Warning: Weather data missing/short")
            logger.warning(resp.content)
            return

        current_weather = json.loads(resp.content.decode('utf-8'))
        logger.debug(str(current_weather))
        self.current_weather = current_weather

        self.temp_deg_c = round(current_weather['current']['temp'] - 273, 1)
        self.weather_symbol = current_weather['current']['weather'][0]['id']

# End of EnvObj() class definition

def t_to_mins(time_in_hrs: str) -> int:
    """Convert times from HH:MM format to mins after midnight."""

    try:
        time_in_mins = 60 * int(time_in_hrs[0:2]) + int(time_in_hrs[3:5])
        return time_in_mins
    except Exception:
        return 0

#  End of t_to_mins()

def t_to_hrs(time_in: int) -> str:
    """Convert times from mins after midnight format to HH:MM."""

    try:
        hours = int(time_in // 60)
        mins = int(time_in - hours * 60)
        time_in_hrs = '{:02d}{}{:02d}'.format(hours, ":", mins)
        return time_in_hrs
    except Exception:
        return "00:00"

#  End of t_to_hrs()

def set_mihome_switch(device_id: str, turn_on: bool) -> bool:
    """Operates a MiHome switch on/off."""

    if turn_on:
        sw_cmd = "on"
    else:
        sw_cmd = "off"

    user_id:str = stgs.MiHome.UserID
    api_key:str = stgs.MiHome.key
    url:str = stgs.MiHome.url + "power_"+ sw_cmd

    payload = {
        "id" : int(device_id),
    }

    # Delay to avoid dropped commands at MiHome server and interference between base stations
    time.sleep(5)

    try:
        resp = requests.put(url, auth=(user_id, api_key), json=payload, timeout=5)
        resp.raise_for_status()
    except requests.exceptions.RequestException as error:
        logger.error(error)
        return False

    parsed = json.loads(resp.content.decode('utf-8'))
    if parsed['status'] == "success":
        return True
    logger.warning("Failure..."+ url + device_id)
    return False

#  End of set_mihome_switch ()


def put_pv_output():
    """Upload generation/consumption data to PVOutput.org."""

    url = stgs.PVOutput.url + "addstatus.jsp"
    key = stgs.PVOutput.key
    sid = stgs.PVOutput.sid

    post_date = time.strftime("%Y%m%d", time.localtime())
    post_time = time.strftime("%H:%M", time.localtime())

    batt_power_out = ge.batt_power if ge.batt_power > 0 else 0
    batt_power_in = -1 * ge.batt_power if ge.batt_power < 0 else 0
    total_cons = ge.consumption - ge.batt_power
    load_pwr = total_cons if total_cons > 0 else 0

    payload = {
        "t"   : post_time,
        "key" : key,
        "sid" : sid,
        "d"   : post_date
    }

    part_payload = {
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
        "v12" : ge.soc
    }

    payload.update(part_payload)  # Concatenate the data, don't escape ":"
    payload = urlencode(payload, doseq=True, quote_via=lambda x,y,z,w: x)

    time.sleep(2)  # PVOutput has a 1 second rate limit. Avoid any clashes

    if not TEST_MODE:
        try:
            resp = requests.get(url, params=payload, timeout=10)
            resp.raise_for_status()
        except requests.exceptions.RequestException as error:
            logger.warning("PVOutput Write Error "+ LONG_T_NOW_VAR)
            logger.warning(error)
            return()

    logger.info("Data; Write to pvoutput.org; "+ post_date+"; "+ post_time+ "; "+ str(part_payload))
    return()

#  End of put_pv_output()


def balance_loads():
    """control loads, based on schedule, generation, temp, etc."""

    new_virt_sr_ss = env_obj.check_sr_ss()

    # Running total of available power. Positive means export
    net_usage_est = ge.pv_power - ge.consumption

    # First pass: update priority values and make any essential load state changes
    for unique_load in load_obj:
        unique_load.refresh_priority(new_virt_sr_ss)
        if unique_load.priority_change:
            if unique_load.priority == 99:
                net_usage_est -= unique_load.toggle("OFF")
            elif unique_load.priority == 0:
                net_usage_est += unique_load.toggle("ON")
        elif (51 <= unique_load.priority <= 90 and
            unique_load.curr_state == "ON"):  # Active low-priority loads are up for grabs
            net_usage_est -= unique_load.est_power

    # Second pass: if possible, turn on new loads, highest priority first
    for priority in range(1, 90):
        for unique_load in load_obj:
            if (unique_load.priority == priority and
                unique_load.curr_state == "OFF" and
                net_usage_est * -1 >= unique_load.est_power and
                net_usage_est < 0 and ge.soc > 98):  # Capacity exists, turn on load

                net_usage_est += unique_load.toggle("ON")

    # Third pass: Turn off loads to rebalance power, lowest priority first
    for priority in range(90, 1, -1):
        for unique_load in load_obj:
            if (unique_load.priority == priority and
                unique_load.curr_state == "ON" and
                (net_usage_est > 0 or ge.soc < 95)):  # Turn off load

                net_usage_est -= unique_load.toggle("OFF")

#  End of balance_loads()


if __name__ == '__main__':

    # Parse any command-line arguments
    TEST_MODE: bool = False
    DEBUG_MODE: bool = False
    ONCE_MODE: bool = False

    MESSAGE = ""
    if len(sys.argv) > 1:
        if str(sys.argv[1]) in ["-t", "--test"]:
            TEST_MODE = True
            DEBUG_MODE = True
            MESSAGE = "Running in test mode... 5 sec loop time, no external server writes"
        elif str(sys.argv[1]) in ["-d", "--debug"]:
            DEBUG_MODE = True
            MESSAGE = "Running in debug mode, extra verbose"
        elif str(sys.argv[1]) in ["-o", "--once"]:
            ONCE_MODE = True
            MESSAGE = "Running in once mode, execute forecast and inverter SoC update, then exit"

    logging.getLogger("matplotlib").setLevel(logging.WARNING)  # Matplotlib is too chatty
    logging.getLogger("PIL").setLevel(logging.WARNING)  # PIL is too chatty
    if DEBUG_MODE:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("PALM")
    #logger.basicConfig(filename='palm_log_test.txt', encoding='utf-8', level=logger.DEBUG)

    logger.critical("PALM... PV Automated Load Manager Version: "+ PALM_VERSION)
    logger.critical("Command line options (only one can be used):")
    logger.critical("-t | --test  | test mode (12x speed, no external server writes)")
    logger.critical("-d | --debug | debug mode, extra verbose")
    logger.critical("-o | --once  | once mode, updates inverter SoC target and then exits")
    logger.critical("")
    if MESSAGE != "":
        logger.critical(MESSAGE)

    LOOP_COUNTER_VAR: int = 0  # 1 minute minor frame. "0" = initialise
    PVO_TSTAMP_VAR: int = 0  # Records value of LOOP_COUNTER_VAR when PV data last written
    MANUAL_HOLD_VAR: bool = False  # Fix for inverter hunting after hitting SoC target

    while True:  # Main Loop
        # Current time definitions
        LONG_T_NOW_VAR: str = time.strftime("%d-%m-%Y %H:%M:%S %z", time.localtime())
        MNTH_VAR: str = LONG_T_NOW_VAR[3:5]
        T_NOW_VAR: str = LONG_T_NOW_VAR[11:]
        T_NOW_MINS_VAR: int = t_to_mins(T_NOW_VAR)

        if LOOP_COUNTER_VAR == 0:  # Initialise
            logger.critical("Initialising at: "+ LONG_T_NOW_VAR)
            logger.critical("")
            sys.stdout.flush()

            # GivEnergy power object
            ge: GivEnergyObj = GivEnergyObj()
            time.sleep(10)

            if MNTH_VAR in stgs.GE.winter:
                ge.set_mode("set_soc_winter")
            else:
                ge.set_mode("set_soc","100")

            # Solcast PV prediction object
            solcast: SolcastObj = SolcastObj()

            # Misc environmental data: weather, CO2, etc
            CO2_USAGE_VAR: int = 200
            env_obj: EnvObj = EnvObj()
            if stgs.CarbonIntensity.enable is True:
                env_obj.update_co2()
            if stgs.OpenWeatherMap.enable is True:
                env_obj.update_weather_curr()

            # Create an object for each load
            if ONCE_MODE is False and stgs.MiHome.enable is True:
                load_obj: List[LoadObj] = []
                load_payload: List[str] = []
                NUM_LOADS: int = len(stgs.LOAD_CONFIG['LoadPriorityOrder'])
                for LOAD_INDEX_VAR in range(NUM_LOADS):
                    load_payload = stgs.LOAD_CONFIG[stgs.LOAD_CONFIG \
                        ['LoadPriorityOrder'][LOAD_INDEX_VAR]]
                    load_obj.append(LoadObj(LOAD_INDEX_VAR, load_payload))

        else:
            # Schedule activities at specific intervals

            # 5 minutes before off-peak start for next day's forecast
            if ((TEST_MODE or ONCE_MODE) and LOOP_COUNTER_VAR == 1) or \
                T_NOW_MINS_VAR == (t_to_mins(stgs.GE.start_time) + 1435) % 1440:
                try:
                    solcast.update()
                except Exception:
                    logger.warning("Warning; Solcast download failure")

            # 2 minutes before off-peak start for setting overnight battery charging target
            if ((TEST_MODE or ONCE_MODE) and LOOP_COUNTER_VAR == 2) or \
                T_NOW_MINS_VAR == (t_to_mins(stgs.GE.start_time) + 1438) % 1440:
                # compute & set SoC target
                try:
                    ge.get_load_hist()
                    logger.info("Forecast weighting: "+ str(stgs.Solcast.weight))
                    ge.compute_tgt_soc(solcast, stgs.Solcast.weight, True)
                except Exception:
                    logger.error("Warning; unable to set SoC")

                # if running in once mode, quit after inverter SoC update
                if ONCE_MODE:
                    sys.exit()

                # Reset sunrise and sunset for next day
                env_obj.reset_sr_ss()

            # Pause/resume battery once charged to compensate for AC3 inverter bug
            # Covers charge period both in early-morning and spanning midnight
            if MANUAL_HOLD_VAR is False and \
                (t_to_mins(stgs.GE.start_time) < T_NOW_MINS_VAR < t_to_mins(stgs.GE.end_time) or \
                 T_NOW_MINS_VAR > t_to_mins(stgs.GE.start_time) > t_to_mins(stgs.GE.end_time) or \
                 t_to_mins(stgs.GE.start_time) > t_to_mins(stgs.GE.end_time) > T_NOW_MINS_VAR):
                if -2 < (ge.soc - ge.tgt_soc) < 2:  # Within 2% avoids sampling issues
                    ge.set_mode("pause")
                    MANUAL_HOLD_VAR = True
            if MNTH_VAR not in stgs.GE.winter and T_NOW_MINS_VAR == t_to_mins(stgs.GE.end_time) or \
                MNTH_VAR in stgs.GE.winter and T_NOW_MINS_VAR == t_to_mins(stgs.GE.end_time_winter):
                ge.set_mode("resume")
                MANUAL_HOLD_VAR = False

            # Afternoon battery boost in winter months to load shift from peak period
            if MNTH_VAR in stgs.GE.winter and stgs.GE.boost_start != "":
                if T_NOW_MINS_VAR == t_to_mins(stgs.GE.boost_start) and \
                    env_obj.co2_high:
                    logger.info("Enabling afternoon battery boost")
                    ge.set_mode("charge_now")
                if T_NOW_MINS_VAR == t_to_mins(stgs.GE.boost_finish):
                    ge.set_mode("set_soc_winter")

            if ONCE_MODE is False:
                # Update carbon intensity every 15 mins as background task
                if stgs.CarbonIntensity.enable is True and LOOP_COUNTER_VAR % 15 == 14:
                    do_get_carbon_intensity = threading.Thread(target=env_obj.update_co2())
                    do_get_carbon_intensity.daemon = True
                    do_get_carbon_intensity.start()

                # Update weather every 15 mins as background task
                if stgs.OpenWeatherMap.enable is True and LOOP_COUNTER_VAR % 15 == 14:
                    do_get_weather = threading.Thread(target=env_obj.update_weather_curr())
                    do_get_weather.daemon = True
                    do_get_weather.start()

                #  Refresh utilisation data from GivEnergy server. Check every minute
                ge.get_latest_data()
                CO2_USAGE_VAR = int(env_obj.co2_intensity * ge.grid_power / 1000)

                #  Turn loads on or off. Check every minute
                if stgs.MiHome.enable is True:
                    do_balance_loads = threading.Thread(target=balance_loads())
                    do_balance_loads.daemon = True
                    do_balance_loads.start()

                # Publish data to PVOutput.org every 5 minutes (or 5 cycles as a catch-all)
                if stgs.PVOutput.enable is True and \
                    (TEST_MODE or \
                    T_NOW_MINS_VAR % 5 == 3 or \
                    LOOP_COUNTER_VAR > PVO_TSTAMP_VAR + 4):

                    PVO_TSTAMP_VAR = LOOP_COUNTER_VAR
                    do_put_pv_output = threading.Thread(target=put_pv_output)
                    do_put_pv_output.daemon = True
                    do_put_pv_output.start()

        LOOP_COUNTER_VAR += 1

        if T_NOW_MINS_VAR == 0:  # Reset frame counter every 24 hours
            ge.pv_energy = 0  # Reset daily to prevent carry-over issue with PVOutput
            ge.grid_energy = 0
            LOOP_COUNTER_VAR = 1

        if TEST_MODE or ONCE_MODE:  # Wait 5 seconds
            time.sleep(5)
        else:  # Sync to minute rollover on system clock
            CURRENT_MINUTE = int(time.strftime("%M", time.localtime()))
            while int(time.strftime("%M", time.localtime())) == CURRENT_MINUTE:
                time.sleep(10)

        sys.stdout.flush()
# End of main
