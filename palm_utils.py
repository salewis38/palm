#!/usr/bin/env python3
"""PALM - PV Active Load Manager."""

import time
import json
from datetime import datetime, timedelta
from typing import Tuple, List
import logging
## import matplotlib.pyplot as plt
import requests
import palm_settings as stgs

logger = logging.getLogger(__name__)

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
# v1.1.0    06/Aug/23 Split out generic functions as palm_utils.py, remove randomised start time
# v1.1.0a   11/Nov/23 Fixed resume operation after daytime charging, bugfix for chart generation
# v1.1.1    23/Mar/24 Improved SoC calcs with additional backward pass to determine min_charge
# v1.1.2    12/Apr/24 Added extra GivEnergy API commands
# v1.1.3    12/May/24 Added get_presets to GE class
# v1.1.4    09/Jul/25 Relaxed response code checking from Givenergy server

PALM_VERSION = "v1.1.4"
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
        self.tgt_soc: int = 100
        self.cmd_list = stgs.GE_Command_list['data']
        self.plot = [""] * 5

        logger.debug("Valid inverter commands:")
        for line in self.cmd_list:
            logger.debug(str(line['id'])+ "- "+ str(line['name']))

    def get_presets(self):
        """Returns list of valid API commands"""

        url = stgs.GE.url + "settings"
        key = stgs.GE.key
        headers = {
            'Authorization': 'Bearer  ' + key,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            resp = requests.request('GET', url, headers=headers, timeout=10)
        except requests.exceptions.RequestException as error:
            logger.error(error)
            return
        return json.dumps(resp.json(), indent = 4)


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
                resp = requests.request('GET', url, headers=headers, timeout=10)
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
                            logger.error("Error reading GivEnergy sys status "+ stgs.pg.t_now)
                            logger.error(resp.content)
                            self.sys_status[i] = self.sys_status[i + 1]
                if stgs.pg.loop_counter == 0:  # Pack array on startup
                    i = 1
                    while i < 5:
                        self.sys_status[i] = self.sys_status[0]
                        i += 1

                self.read_time_mins = t_to_mins(self.sys_status[0]['time'][11:])
                # Check for BST and convert to local time
                if time.strftime("%z", time.localtime()) == "+0100":
                    self.read_time_mins = (self.read_time_mins + 60) % 1440

                self.line_voltage = float(self.sys_status[0]['grid']['voltage'])
                self.grid_power = -1 * int(self.sys_status[0]['grid']['power'])  # -ve = export
                self.pv_power = int(self.sys_status[0]['solar']['power'])
                self.batt_power = int(self.sys_status[0]['battery']['power'])  # -ve = charging
                self.consumption = int(self.sys_status[0]['consumption'])
                self.soc = int(self.sys_status[0]['battery']['percent'])

            url = stgs.GE.url + "meter-data/latest"
            try:
                resp = requests.request('GET', url, headers=headers, timeout=10)
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
                            logger.error("Error reading GivEnergy meter status "+ stgs.pg.t_now)
                            logger.error(resp.content)
                            self.meter_status[i] = self.meter_status[i + 1]
                if stgs.pg.loop_counter == 0:  # Pack array on startup
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
            day_delta = offset if (stgs.pg.t_now_mins > 1260) else offset + 1  # Today if >9pm
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
                resp = requests.request('GET', url, headers=headers, params=params, timeout=10)
            except requests.exceptions.RequestException as error:
                logger.error(error)
                return load_array
            if 299 < resp.status_code < 200:
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
        total_weight: float = 0

        i: int = 0
        while i < len(stgs.GE.load_hist_weight):
            if stgs.GE.load_hist_weight[i] > 0:
                logger.debug("Processing load history for day -"+ str(i + 1))
                load_hist_array = get_load_hist_day(i)
                j = 0
                while j < 48:
                    acc_load[j] += load_hist_array[j] * stgs.GE.load_hist_weight[i]
                    acc_load[j] = round(acc_load[j], 2)
                    j += 1
                total_weight += stgs.GE.load_hist_weight[i]
                logger.debug(str(acc_load)+ " total weight: "+ str(total_weight))
            else:
                logger.debug("Skipping load history for day -"+ str(i + 1)+ " (weight <= 0)")
            i += 1

        # Avoid DIV/0 if config file contains incorrect weightings
        if total_weight == 0:
            logger.error("Configuration error: incorrect daily weightings")
            total_weight = 1

        # Calculate averages and write results
        i = 0
        while i < 48:
            self.base_load[i] = round(acc_load[i]/total_weight, 1)
            i += 1
        logger.debug("Load Calc Summary: "+ str(self.base_load))

    def set_mode(self, cmd: str):
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
            if not stgs.pg.test_mode:
                try:
                    resp = requests.request('POST', url, headers=headers, json=payload, timeout=10)
                except requests.exceptions.RequestException as error:
                    logger.error(error)
                    return
                if 299 < resp.status_code < 200:
                    logger.info("Invalid response: "+ str(resp.status_code))
                    return

            logger.info("Setting Register "+ str(register)+ " ("+ str(cmd_name) + ") to "+
                        str(value)+ "   Response: "+ str(resp))

            time.sleep(3)  # Allow data on GE server to settle

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
                resp = requests.request('POST', url, headers=headers, json=payload, timeout=10)
            except resp.exceptions.RequestException as error:
                logger.error(error)
                return
            if 299 < resp.status_code < 200:
                logger.error("Invalid response: "+ str(resp.status_code))
                return

            returned_cmd = json.loads(resp.content.decode('utf-8'))['data']['value']
            if str(returned_cmd) == str(value):
                logger.info("Successful register read: "+ str(register)+ " = "+ str(returned_cmd))
            else:
                logger.error("Readback failed on GivEnergy API... Expected " +
                    str(value) + ", Read: "+ str(returned_cmd))

        if cmd == "charge_now":   # Replicates App command for CHARGE AT FULL POWER
            set_inverter_register("64", "00:01")
            set_inverter_register("65", "23:59")
            set_inverter_register("66", "true")
            set_inverter_register("77", "100")
            set_inverter_register("72", "3000")

        elif cmd == "charge_now_soc":
            set_inverter_register("77", str(self.tgt_soc))
            set_inverter_register("64", "00:01")
            set_inverter_register("65", "23:59")

        elif cmd == "discharge_now":  # Replicates App command for DISCHARGE AT FULL POWER
            set_inverter_register("24", "false")
            set_inverter_register("53", "00:01")
            set_inverter_register("54", "23:59")
            set_inverter_register("56", "true")
            set_inverter_register("66", "false")
            set_inverter_register("73", "3000")

        elif cmd == "pause":
            set_inverter_register("72", "0")
            set_inverter_register("73", "0")

        elif cmd == "pause_charge":
            set_inverter_register("72", "0")

        elif cmd == "pause_discharge":
            set_inverter_register("73", "0")

        elif cmd == "play":  # Replicates App commands for PLAY
            set_inverter_register("24", "true")
            set_inverter_register("53", "00:01")
            set_inverter_register("54", "23:59")
            set_inverter_register("56", "false")
            set_inverter_register("66", "true")
            set_inverter_register("72", "3000")
            set_inverter_register("73", "3000")
            self.set_mode("set_soc")

        elif cmd == "resume":  # Alias for legacy instruction
            self.set_mode("play")

        elif cmd == "set_soc":  # Sets target SoC to value
            set_inverter_register("77", str(self.tgt_soc))
            if stgs.GE.start_time != "":
                start_time = t_to_hrs(t_to_mins(stgs.GE.start_time))
                set_inverter_register("64", start_time)
            if stgs.GE.end_time != "":
                set_inverter_register("65", stgs.GE.end_time)

        elif cmd == "set_soc_winter":  # Restore default overnight charge params
            set_inverter_register("77", "100")
            if stgs.GE.start_time != "":
                start_time = t_to_hrs(t_to_mins(stgs.GE.start_time))
                set_inverter_register("64", stgs.GE.start_time)
            if stgs.GE.end_time_winter != "":
                set_inverter_register("65", stgs.GE.end_time_winter)

        elif cmd == "test":
            logger.debug("Test set_mode")

        else:
            logger.error("unknown inverter command: "+ cmd)

    def compute_tgt_soc(self, gen_fcast, weight: int, commit: bool) -> str:
        """Compute overnight SoC target"""

        # Winter months = 100%, no need for sums
        if stgs.pg.test_mode is False and stgs.pg.month in stgs.GE.winter and commit:
            logger.info("winter month, SoC set to 100")
            self.tgt_soc = 100
            return "set_soc_winter"

        # Quick check for valid generation data
        if gen_fcast.pv_est50_day[0] == 0:
            logger.error("Missing generation data, SoC set to 100")
            self.tgt_soc = 100
            return "set_soc"

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
        logger.info("{:<20} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}".\
            format("SoC Calc;", "Day", "Hour", "Charge", "Cons", "Gen", "SoC", "Min", "Max"))

        # Definitions for export of SoC forecast in chart form
        tgt_time = ["Time"]
        tgt_soc_raw = ["Calculated SoC"]
        tgt_soc_adj = ["Adjusted SoC"]
        tgt_max_line = ["Max"]
        tgt_rsv_line = ["Reserve"]

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
        # for maximum charge and also the minimum charge that occurs before the maximum.

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
                    total_load = self.base_load[i]
                    est_gen = (gen_fcast.pv_est10_30[day*48 + i] * wgt_10 +
                        gen_fcast.pv_est50_30[day*48 + i] * wgt_50 +
                        gen_fcast.pv_est90_30[day*48 + i] * wgt_90) / (wgt_10 + wgt_50 + wgt_90)
                    batt_charge[i] = (batt_charge[i - 1] +
                        max(-1 * stgs.GE.charge_rate,
                        min(stgs.GE.charge_rate, (est_gen - total_load))))

                # Forward pass: Capture min charge before charge exceeds overnight value
                # and max charge during the day.
                # At this point in the code, min_charge is the last minimum before the charge
                # exceeds the overnight value. It may not be the last, that needs a second pass.
                if batt_charge[i] < batt_charge[i-1] and max_charge <= reserve_energy:
                    min_charge = min(min_charge, batt_charge[i])
                elif i > end_charge_period:  # Charging after overnight boost
                    max_charge = max(max_charge, batt_charge[i])

                logger.info("{:<20} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}".\
                    format("SoC Calc;", \
                        day, t_to_hrs(i * 30), \
                        round(batt_charge[i], 2), \
                        round(total_load, 2), round(est_gen, 2), \
                        int(100 * batt_charge[i] / batt_max_charge), \
                        int(100 * min_charge/batt_max_charge), \
                        int(100 * max_charge/batt_max_charge)))

                # These arrays are used for the second pass and to plot the workings (if needed)
                tgt_time.append(t_to_hrs((day*48 + i) * 30))  # Time
                tgt_soc_raw.append(int(100 * batt_charge[i]/batt_max_charge))  # Baseline SoC line
                tgt_max_line.append(100)  # Upper limit line for chart readability
                tgt_rsv_line.append(stgs.GE.batt_reserve)  # Lower limit line

                i += 1

            max_charge_pcnt[day] = int(100 * max_charge / batt_max_charge)
            min_charge_pcnt[day] = int(100 * min_charge / batt_max_charge)

            day += 1

        # Backward pass. The min charge value above is the first of the day, there may be others
        # Search the SoC data from the max point backwards to find the true minimum
        day = 0
        while day < 2:  # Repeat for tomorrow and next day
            i = 47 + day * 48
            search_window = False
            while i > day * 48:
                if int(tgt_soc_raw[i]) == int(max_charge_pcnt[day]):
                    search_window = True
                if search_window:
                    min_charge_pcnt[day] = min(int(min_charge_pcnt[day]), int(tgt_soc_raw[i]))
                i -= 1
            day += 1

        logger.info("SoC Calc; Min (day 0, day 1) = "+\
            str(min_charge_pcnt[0])+ ", "+ str(min_charge_pcnt[1]))
        logger.info("SoC Calc; Max (day 0, day 1) = "+\
            str(max_charge_pcnt[0])+ ", "+ str(max_charge_pcnt[1]))

        # We now have the four values of max & min charge for tomorrow & overmorrow
        # Check if overmorrow is better than tomorrow and there is opportunity to reduce target
        # to avoid residual charge at the end of the day in anticipation of a sunny day.
        # Do this by implying that there will be more than forecast generation
        max_charge_pc = max_charge_pcnt[0]  # Working value
        min_charge_pc = min_charge_pcnt[0]  # Working value

        if max_charge_pcnt[1] > 100 and  max_charge_pcnt[0] < 100:
            logger.info("SoC Calc; Overmorrow correction enabled")
            max_charge_pc += int((max_charge_pcnt[1] - 100) / 2)
        else:
            logger.info("SoC Calc; Overmorrow correction not needed/enabled")

        # low_soc is the minimum SoC target. Provide more buffer capacity in shoulder months
        # when load is likely to be more variable, e.g. heating
        if stgs.pg.month in stgs.GE.shoulder:
            low_soc = int(stgs.GE.max_soc_target)
        else:
            low_soc = int(stgs.GE.min_soc_target)

        # The really clever bit: reduce the target SoC to the greater of:
        #     The surplus above 100% for max_charge_pcnt
        #     The value needed to achieve the stated spare capacity at minimum charge point
        #     The preset minimum value
        tgt_soc = max(100 - max_charge_pc, (low_soc - min_charge_pc), low_soc)
        # Range check the resulting value
        tgt_soc = int(min(tgt_soc, 100))  # Limit range to 100%

        # Produce plot of adjusted SoC
        day = 0
        diff = tgt_soc
        while day < 2:
            i = 0
            while i < 48:
                if day == 1 and i == 0:
                    diff = tgt_soc_adj[48] - tgt_soc_raw[49]
                if tgt_soc_raw[day*48 + i + 1] + diff > 100:  # Correct for SoC > 100%
                    diff = 100 - tgt_soc_raw[day*48 + i + 1]  # Bugfix v1.1.0a
                tgt_soc_adj.append(tgt_soc_raw[day*48 + i + 1] + diff)
                i += 1
            day += 1

        # Store plot data
        self.plot[0] = str(tgt_time)
        self.plot[1] = str(tgt_soc_raw)
        self.plot[2] = str(tgt_soc_adj)
        self.plot[3] = str(tgt_max_line)
        self.plot[4] = str(tgt_rsv_line)

        logger.info("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("SoC Calc Summary;",
            "Max Charge", "Min Charge", "Max %", "Min %", "Target SoC"))
        logger.info("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("SoC Calc Summary;",
            round(max_charge, 2), round(min_charge, 2),
            max_charge_pcnt[0], min_charge_pcnt[0], "N/A"))
        logger.info("{:<25} {:>10} {:>10} {:>10} {:>10} {:>10}".format("SoC (Adjusted);",
            round(max_charge, 2), round(min_charge, 2),
            max_charge_pc + tgt_soc, min_charge_pc + tgt_soc, tgt_soc))

        if commit:  # Issue command to set new SoC and exit
            self.tgt_soc = tgt_soc
            return "set_soc"

        return "test"

# End of GivEnergyObj() class definition

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
            if 299 < resp.status_code < 200:
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
            forecast_lines = min(len(solcast_data_1['forecasts']), \
                len(solcast_data_2['forecasts'])) - 1
        else:
            forecast_lines = len(solcast_data_1['forecasts']) - 1
        interval = int(solcast_data_1['forecasts'][0]['period'][2:4])
        solcast_offset = t_to_mins(solcast_data_1['forecasts'][0]['period_end'][11:16]) \
            - interval - 60

        # Check for BST and convert to local time to align with GivEnergy data
        if time.strftime("%z", time.localtime()) == "+0100":
            logger.info("Applying BST offset to Solcast data")
            solcast_offset += 60

        i = solcast_offset
        cntr = 0
        while i < solcast_offset + forecast_lines * interval:
            try:
                pv_est10[i] = int(solcast_data_1['forecasts'][cntr]['pv_estimate10'] * 1000)
                pv_est50[i] = int(solcast_data_1['forecasts'][cntr]['pv_estimate'] * 1000)
                pv_est90[i] = int(solcast_data_1['forecasts'][cntr]['pv_estimate90'] * 1000)
            except Exception:
                logger.error("Error: Unexpected end of Solcast data (array #1). i="+ \
                    str(i)+ "cntr="+ str(cntr))
                break

            if i > 1 and i % interval == 0:
                cntr += 1
            i += 1

        if stgs.Solcast.url_sw != "":  # Two arrays are specified
            i = solcast_offset
            cntr = 0
            while i < solcast_offset + forecast_lines * interval:
                try:
                    pv_est10[i] += int(solcast_data_2['forecasts'][cntr]['pv_estimate10'] * 1000)
                    pv_est50[i] += int(solcast_data_2['forecasts'][cntr]['pv_estimate'] * 1000)
                    pv_est90[i] += int(solcast_data_2['forecasts'][cntr]['pv_estimate90'] * 1000)
                except Exception:
                    logger.error("Error: Unexpected end of Solcast data (array #2). i="+ \
                        str(i)+ "cntr="+ str(cntr))
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

# End of palm_utils
