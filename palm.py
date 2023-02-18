#!/usr/bin/env python3
"""PALM - PV Active Load Manager."""

import sys
import time
import threading
import json
from datetime import datetime, timedelta
from typing import Tuple, List
from urllib.parse import urlencode
import requests
from scipy.stats import rankdata
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
# v0.8.4d   09/Jan/23 Updated GivEnergyObj to download & validate inverter commands
# v0.9.0    28/Jan/23 New format scheduler

PALM_VERSION = "v0.9.0"
# -*- coding: utf-8 -*-

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
        self.cmd_list = stgs.GE.valid_cmds
        self.inverter_registers = [""] * 100

        #  Download valid commands for inverter
        url = stgs.GE.url + "settings"
        key = "Bearer " + stgs.GE.key

        payload = {}
        headers = {
                'Authorization': key,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
                }

        try:
            resp = requests.request('GET', url, headers=headers, json=payload)
        except resp.exceptions.RequestException as error:
            print(error)
            return
        if resp.status_code != 200:
            print("Invalid response:", resp.status_code)
            return

        self.cmd_list = json.loads(resp.content.decode('utf-8'))['data']

        self.set_mode("INITIALISE", "0", "0")

        if DEBUG_MODE:
            print()
            print("Inverter command list donwloaded")
            print("Valid commands:")
            for line in self.cmd_list:
                print(line['id'], " - ", line['name'])
            print("")

    def get_latest_usage(self):
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
            except resp.exceptions.RequestException as error:
                print(error)
                return
            if resp.status_code != 200:
                print("Invalid response:", resp.status_code)
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
                            print("Error reading GivEnergy system status ", TIME_NOW_VAR)
                            print(resp.content)
                            self.sys_status[i] = self.sys_status[i + 1]

                if LOOP_COUNTER_VAR == 0:  # Pack array on startup
                    i = 1
                    while i < 5:
                        self.sys_status[i] = self.sys_status[0]
                        i += 1
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
            except resp.exceptions.RequestException as error:
                print(error)
                return
            if resp.status_code != 200:
                print("Invalid response:", resp.status_code)
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
                            print("Error reading GivEnergy meter status ", TIME_NOW_VAR)
                            print(resp.content)
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
            'pageSize': '2000'
        }

        try:
            resp = requests.request('GET', url, headers=headers, params=params)
        except resp.exceptions.RequestException as error:
            print(error)
            return ""
        if resp.status_code != 200:
            print("Invalid response:", resp.status_code)
            return ""

        base_load = []

        if len(resp.content) > 100:
            history = json.loads(resp.content.decode('utf-8'))
            i = 6
            current_energy = prev_energy = 0
            while i < 290:
                try:
                    current_energy = float(history['data'][i]['today']['consumption'])
                except Exception:
                    current_energy = prev_energy
                base_load.append(round(current_energy - prev_energy, 1))
                prev_energy = current_energy
                i += 6
            if DEBUG_MODE:
                print("Info; Load Calc Summary:", current_energy, base_load)

        return base_load

    def set_mode(self, cmd: str, arg_reserve: str, arg_soc: str):
        """Configures inverter operating mode"""

        def set_inverter_register(register: str, value: str):
            """Exactly as it says"""

            resp = ""
            valid_cmd = False
            for line in self.cmd_list:
                if line['id'] == int(register):
                    cmd_name = line['name']
                    valid_cmd = True
                    break

            # Only write if a valid command and a change of register state
            if valid_cmd and self.inverter_registers[int(register)] != value:
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

                if not TEST_MODE:
                    try:
                        resp = requests.request('POST', url, headers=headers, json=payload)
                    except requests.exceptions.RequestException as error:
                        print(error)
                        return
                    if resp.status_code != 200:
                        print("Invalid response:", resp.status_code)
                        return
                else:
                    resp = "TEST"

                print("Info; Setting Register ", register, " (", cmd_name, ") to ", value, ", \
                    Response:", resp, sep='')

                time.sleep(2)

                # Readback
                url = stgs.GE.url + "settings/" + register + "/read"
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
                    print(error)
                    return
                if resp.status_code != 201:
                    print("Invalid response:", resp.status_code)
                    return

                returned_cmd = json.loads(resp.content.decode('utf-8'))['data']['value']
                if str(returned_cmd) == str(value):
                    print("Info; Successful register read", register, "=", returned_cmd)
                    self.inverter_registers[int(register)] = value
                else:
                    print("Error: writing to GivEnergy via API... expected: ", \
                        value, "Read: ", returned_cmd)
            if not valid_cmd:
                print("Error: write attempt to invalid inverter register: ", register)

        print("GE inverter mode:", cmd)
        if cmd == "INITIALISE":
            set_inverter_register("17", "True")
            set_inverter_register("24", "True")
            set_inverter_register("53", "00:00")
            set_inverter_register("54", "00:00")
            set_inverter_register("56", "False")
            set_inverter_register("64", "00:00")
            set_inverter_register("65", "00:00")
            set_inverter_register("66", "True")
            set_inverter_register("71", arg_reserve)
            set_inverter_register("72", "3000")
            set_inverter_register("73", "3000")
            set_inverter_register("77", "100")

        elif cmd == "TARGET":  # Import to target SoC to value, export allowed
            set_inverter_register("56", "False")
            set_inverter_register("64", "00:00")
            set_inverter_register("65", "23:59")
            set_inverter_register("71", arg_reserve)
            set_inverter_register("72", "3000")
            set_inverter_register("73", "3000")
            set_inverter_register("77", arg_soc)

        elif cmd == "IMPORT":  # Import to target SoC to value, no export
            set_inverter_register("56", "False")
            set_inverter_register("64", "00:00")
            set_inverter_register("65", "23:59")
            set_inverter_register("71", arg_reserve)
            set_inverter_register("72", "3000")
            set_inverter_register("73", "3000")
            soc = max(int(ge.soc), int(arg_soc))
            set_inverter_register("77", str(soc))

        elif cmd == "EXPORT":  # Full-rate forceed discharge
            set_inverter_register("53", "00:00")
            set_inverter_register("54", "23:59")
            set_inverter_register("56", "True")
            set_inverter_register("71", arg_reserve)
            set_inverter_register("72", "3000")
            set_inverter_register("73", "3000")

        elif cmd == "ECO":  # Normal operating mode
            set_inverter_register("56", "False")
            set_inverter_register("24", "True")
            set_inverter_register("71", arg_reserve)
            set_inverter_register("72", "3000")
            set_inverter_register("73", "3000")

        elif cmd == "CHARGE":  # ECO with no discharge - save export for later
            set_inverter_register("56", "False")
            set_inverter_register("24", "True")
            set_inverter_register("71", arg_reserve)
            set_inverter_register("72", "3000")
            set_inverter_register("73", "0")

        elif cmd == "DISCHARGE":  # ECO with no charge - export any excess
            set_inverter_register("24", "True")
            set_inverter_register("56", "False")
            set_inverter_register("71", arg_reserve)
            set_inverter_register("72", "0")
            set_inverter_register("73", "3000")

        elif cmd == "HOLD":  # Pause inverter
            set_inverter_register("24", "False")
            set_inverter_register("56", "False")
            set_inverter_register("72", "0")
            set_inverter_register("73", "0")

# Special Cases
        elif cmd == "GO_TGT":  # Used to set Octopus Go
            set_inverter_register("56", "False")
            set_inverter_register("64", stgs.GE.start_time)
            end_time = stgs.GE.end_time_winter if MONTH_VAR in stgs.GE.winter else stgs.GE.end_time
            set_inverter_register("65", end_time)
            set_inverter_register("71", arg_reserve)
            set_inverter_register("72", "3000")
            set_inverter_register("73", "3000")
            set_inverter_register("77", arg_soc)

        elif cmd == "COSY_PM":  # Import to target SoC to value, no export
            set_inverter_register("56", "False")
            set_inverter_register("64", stgs.GE.boost_start)
            set_inverter_register("65", stgs.GE.boost_finish)
            set_inverter_register("71", arg_reserve)
            set_inverter_register("72", "3000")
            set_inverter_register("73", "3000")
            set_inverter_register("77", "100")

        else:
            print("error: unknown inverter command:", cmd)

# End of GivEnergyObj() class definition

class SchedulerObj:
    """24-hour scheduler"""

    def __init__(self):

        self.ttime = [""] * 48  # Time  "HH:MM"
        self.imp_cost = [0] * 48  # Imp Cost p/kWh
        self.imp_cost_rank = [0] * 48  # Rank of Imp Cost
        self.exp_price = [0] * 48  # Exp Price p/kWh
        self.exp_price_rank = [0] * 48  # Rank of Exp Price
        self.load = [0] * 48  # Load kWh
        self.mode = [""] * 48  # Mode list
        self.reserve = [4] * 48  # Reserve
        self.soc = [100] * 48  # Target SoC
        self.inv_mode = [""] * 48  # Inverter mode
        self.inv_soc = [100] * 48  # Inverter target SoC

        # Validate imported schedule
        i = 0
        while i < 48:
            line = stgs.Schedule.sched[i]

            time_mins = i * 30
            hours = int(time_mins // 60)
            mins = int(time_mins - hours * 60)
            time_in_hrs = f"{hours :02d}{':'}{mins :02d}"

            assert line[0] == time_in_hrs, "Error: unexpected value in time column of schedule"
            self.ttime[i] = line[0]
            assert -200 < line[1] < 200, "Error: out of range import cost in schedule"
            self.imp_cost[i] = line[1]
            assert -200 < line[2] < 200, "Error: out of range export price in schedule"
            self.exp_price[i] = line[2]
            assert 0 <= line[3] < 10, "Error: out or range power consumption in schedule"
            self.load[i] = line[3]
            valid_modes = ["TARGET", "IMPORT", "EXPORT", "ECO", \
                "CHARGE", "DISCHARGE", "HOLD", "AGILE", "GO_TGT", "COSY_PM"]
            assert line[4] in valid_modes, "Error: unknown mode in schedule"
            self.mode[i] = line[4]
            assert 0 <= line[5] <= 100, "Error: out of range reserve level in schedule"
            self.reserve[i] = line[5]
            assert 0 <= line[6] <= 100, "Error: out of range SoC target in schedule"
            self.soc[i] = line[6]
            i += 1

    def update(self):
        """Updates with latest prices, power consumption and rankings"""

        if stgs.Octopus.enable_in:
            # Read Agile Octopus tariffs
            # Import rates
            url = stgs.Octopus.incoming_url

            try:
                resp = requests.request('GET', url)
            except resp.exceptions.RequestException as error:
                print(error)
                return
            if resp.status_code != 200:
                print("Invalid response:", resp.status_code)
                return

            time_list = json.loads(resp.content.decode('utf-8'))['results']

            i = 0
            while i < 48:
                for line in time_list:
                    t_time = line['valid_from'][11:16]
                    t_val = round(line['value_inc_vat'], 2)
                    if t_time == self.ttime[i]:
                        self.imp_cost[i] = t_val
                        break
                i += 1

        if stgs.Octopus.enable_out:
            # Export rates
            url = stgs.Octopus.outgoing_url
            try:
                resp = requests.request('GET', url)
            except resp.exceptions.RequestException as error:
                print(error)
                return
            if resp.status_code != 200:
                print("Invalid response:", resp.status_code)
                return

            time_list = json.loads(resp.content.decode('utf-8'))['results']

            i = 0
            while i < 48:
                for line in time_list:
                    t_time = line['valid_from'][11:16]
                    t_val = round(line['value_inc_vat'], 2)
                    if t_time == self.ttime[i]:
                        self.exp_price[i] = t_val
                        break
                i += 1

        # Updates load history using actuals
        self.load = ge.get_load_hist()

        # Add rankings
        imp_rank = rankdata(self.imp_cost, method='ordinal')
        exp_rank = rankdata(self.exp_price, method='ordinal')
        i = 0
        while i < 48:
            self.imp_cost_rank[i] = imp_rank[i]
            self.exp_price_rank[i] = exp_rank[i]
            i += 1

    def display(self):
        """Dumps schedule"""

        print(f"{'Info; Schedule;':<20}{'Hour':>10}{'Imp Cost':>10}{'Rank':>10}{'Exp Cost':>10}", \
            f"{'Rank':>10}{'Load' :>10}{'User Mode':>10}{'Reserve':>10}{'User Soc':>10}", \
            f"{'Inv Mode':>10}{'Inv SoC':>10}")
        i = 0
        while i < 48:
            print(f"{'Info; Schedule;':<20}{self.ttime[i]:>10}{self.imp_cost[i]:>10}", \
                f"{self.imp_cost_rank[i]:>10}{self.exp_price[i]:>10}{self.exp_price_rank[i]:>10}",\
                f"{self.load[i]:>10}{self.mode[i]:>10}{self.reserve[i]:>10}{self.soc[i]:>10}", \
                f"{self.inv_mode[i]:>10}{self.inv_soc[i]:>10}")
            i += 1

    def compute_tgt_soc(self, gen_fcast, weight: int, commit: bool):
        """Compute tomorrow's overnight SoC target"""

        batt_max_charge: float = stgs.GE.batt_max_charge

        weight = min(max(weight,10),90)  # Range check
        wgt_10 = max(0, 50 - weight)  # Triangular approximation to Solcast normal distrbution
        wgt_50 = 90 - weight if weight > 50 else weight - 10
        wgt_90 = max(0, weight - 50)

        tgt_soc = 100
        if gen_fcast.pv_est50_day[0] > 0:

            #  Step through forecast generation and consumption for coming day to identify
            #  lowest minimum before overcharge and maximum overcharge. If there is overcharge
            #  and the first min is above zero, reduce overnight charge for min export.

            batt_charge: float = [0] * 49
            batt_charge[0] = batt_max_charge
            max_charge = 0
            min_charge = batt_max_charge

            print()
            print(f"{'Info; SoC Calcs;':<20}{'Hour':>10}{'Charge':>10}{'Cons':>10}{'Gen':>10}", \
                f"{'SoC':>10}")

            i = 0
            while i < 48:
                if self.mode[i] in ["TARGET", "IMPORT", "HOLD", "CHARGE"]:
                    # Battery not discharging
                    total_load = 0
                else:
                    total_load = float(self.load[i])
                est_gen = (gen_fcast.pv_est10_30[i] * wgt_10 +
                    gen_fcast.pv_est50_30[i] * wgt_50+
                    gen_fcast.pv_est90_30[i] * wgt_90) / (wgt_10 + wgt_50 + wgt_90)
                est_gen /= 2
                if i > 0:
                    batt_charge[i] = (batt_charge[i - 1] +
                        max(-1 * stgs.GE.charge_rate / 2,
                            min(stgs.GE.charge_rate / 2, est_gen - total_load)))
                    # Capture min charge on lowest down-slope before charge exceeds 100%
                    if (batt_charge[i] <= batt_charge[i - 1] and
                        max_charge < batt_max_charge):
                        min_charge = min(min_charge, batt_charge[i])
                    elif i > 4:  # Charging after overnight boost
                        max_charge = max(max_charge, batt_charge[i])
                print(f"{'Info; SoC Calc;':<20}{time_to_hrs(60 * i/2):>10}", \
                    f"{round(batt_charge[i], 2):>10}{round(total_load, 2):>10}", \
                    f"{round(est_gen, 2):>10}{int(100 * batt_charge[i] / batt_max_charge):>10}")
                i += 1

            max_charge_pcnt = int(100 * max_charge / batt_max_charge)
            min_charge_pcnt = int(100 * min_charge / batt_max_charge)

            #  Reduce nightly charge to capture max export
            if MONTH_VAR in stgs.GE.winter:
                tgt_soc = 100
            elif MONTH_VAR in stgs.GE.shoulder:
                tgt_soc = max(stgs.GE.max_soc_target, 100 - min_charge_pcnt,
                    200 - max_charge_pcnt)
            else:
                tgt_soc = max(stgs.GE.min_soc_target, 100 - min_charge_pcnt,
                                200 - max_charge_pcnt)
            tgt_soc = int(min(max(tgt_soc, 0), 100))  # Limit range

            print()
            print(f"{'Info; SoC Calc Summary;':<25}{'Max Charge':>10}{'Min Charge':>10}", \
                f"{'Max %':>10} {'Min %':>10} {'Target SoC':>10}")
            print(f"{'Info; SoC Calc Summary;':<25}{round(max_charge, 2):>10}", \
                f"{round(min_charge, 2):>10}{max_charge_pcnt:>10}{min_charge_pcnt:>10}", \
                f"{tgt_soc:>10}")
        else:
            print("Info; Incomplete Solcast data, setting target SoC to 100%")

        print("Info; SoC Summary; ", LONG_TIME_NOW_VAR, "; Tom Fcast Gen (kWh); ",
            gen_fcast.pv_est10_day[0], ";", gen_fcast.pv_est50_day[0], ";",
            gen_fcast.pv_est90_day[0], "; SoC Target (%); ", tgt_soc,
            "; Today Gen (kWh); ", round(ge.pv_energy) / 1000, 2)
        print()

        if commit:
            i = 0
            while i < 48:
                # Copy mode directly from input table if not Agile mode
                if self.mode[i] == "AGILE":
                    # Find mid-value
                    j = 0
                    while j < 48:
                        if self.imp_cost_rank[j] == 24:
                            mid_price = self.imp_cost[j]
                            break
                        j += 1
                    # Import at up to 12 cheapest time slots, but only if 20% below average cost
                    if self.imp_cost_rank[i] < 13 and self.imp_cost[i] < 0.8*sum(self.imp_cost)/48:
                        self.inv_mode[i] = "IMPORT"
                        self.inv_soc[i] = 100
                    # Charge excess and discharge as needed when at or above mid-point price
                    elif self.imp_cost[i] >= mid_price:
                        self.inv_mode[i] = "ECO"
                        self.inv_soc[i] = 100
                    # No discharge at marginal times
                    else:
                        self.inv_mode[i] = "CHARGE"
                        self.inv_soc[i] = 100

                elif self.mode[i] == "COSY_PM":
                    self.inv_mode[i] = self.mode[i]
                    self.inv_soc[i] = "100"

                elif self.mode[i] == "GO_TGT":
                    self.inv_mode[i] = self.mode[i]
                    self.inv_soc[i] = str(tgt_soc) if self.soc[i] == 0 else self.soc[i]

                else:
                    self.inv_mode[i] = self.mode[i]
                    self.inv_soc[i] = str(tgt_soc)
                i += 1

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
            valid_time = (self.early_start_mins <= TIME_NOW_MINS_VAR or
                TIME_NOW_MINS_VAR < self.finish_time_mins)

        # Force a start if load has timed-out with run time below its daily target
        late_start_active = (self.late_start_mins < TIME_NOW_MINS_VAR and
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
        self.pv_est50_day: [int] = [0] *  7
        self.pv_est90_day: [int] = [0] * 7

        self.pv_est10_30: [int] = [0] * 48
        self.pv_est50_30: [int] = [0] * 48
        self.pv_est90_30: [int] = [0] * 48

    def update(self):
        """Updates forecast generation from Solcast."""

        def get_solcast(url) -> Tuple[bool, str]:
            """Download latest Solcast forecast."""

            solcast_url = url + stgs.Solcast.cmd + "&api_key=" + stgs.Solcast.key
            try:
                resp = requests.get(solcast_url, timeout=5)
                resp.raise_for_status()
            except resp.exceptions.RequestException as error:
                print(error)
                return False, ""
            if resp.status_code != 200:
                print("Invalid response:", resp.status_code)
                return False, ""

            if len(resp.content) < 50:
                print("Warning: Solcast data missing/short")
                print(resp.content)
                return False, ""

            solcast_data = json.loads(resp.content.decode('utf-8'))
            if TEST_MODE:
                print(solcast_data)

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

        # v0.8.3b bugfix: Number of lines reduced by 1 in Solcast data
        forecast_lines = min(len(solcast_data_1['forecasts']), len(solcast_data_2['forecasts'])) - 1
        interval = int(solcast_data_1['forecasts'][0]['period'][2:4])
        solcast_offset = (60 * int(solcast_data_1['forecasts'][0]['period_end'][11:13]) +
            int(solcast_data_1['forecasts'][0]['period_end'][14:16]) - interval - 60)

        i = solcast_offset
        cntr = 0
        while i < forecast_lines * interval:
            pv_est10[i] = (int(solcast_data_1['forecasts'][cntr]['pv_estimate10'] * 1000) +
                int(solcast_data_2['forecasts'][cntr]['pv_estimate10'] * 1000))

            pv_est50[i] = (int(solcast_data_1['forecasts'][cntr]['pv_estimate'] * 1000) +
                int(solcast_data_2['forecasts'][cntr]['pv_estimate'] * 1000))

            pv_est90[i] = (int(solcast_data_1['forecasts'][cntr]['pv_estimate90'] * 1000) +
                int(solcast_data_2['forecasts'][cntr]['pv_estimate90'] * 1000))

            if i > 1 and i % interval == 0:
                cntr += 1
            i += 1

        i = 0
        if solcast_offset > 720:  # Forget obout current day
            offset = 1440 - 90
        else:
            offset = 0

        while i < 7:  # Summarise daily forecasts
            start = i * 1440 + offset + 1
            end = start + 1439
            self.pv_est10_day[i] = round(sum(pv_est10[start:end]) / 60000, 3)
            self.pv_est50_day[i] = round(sum(pv_est50[start:end]) / 60000, 3)
            self.pv_est90_day[i] = round(sum(pv_est90[start:end]) / 60000, 3)
            i += 1

        i = 0
        while i < 48:  # Calculate half-hourly generation
            start = i * 30 + offset + 1
            end = start + 29
            self.pv_est10_30[i] = round(sum(pv_est10[start:end])/30000, 3)
            self.pv_est50_30[i] = round(sum(pv_est50[start:end])/30000, 3)
            self.pv_est90_30[i] = round(sum(pv_est90[start:end])/30000, 3)
            i += 1

        timestamp = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime())
        print("Info; PV Estimate 10% (30 mins, 7 days) / kWh", ";", timestamp, ";",
            self.pv_est10_30[0:47], self.pv_est10_day[0:6])
        print("Info; PV Estimate 50% (30 mins, 7 days) / kWh", ";", timestamp, ";",
            self.pv_est50_30[0:47], self.pv_est50_day[0:6])
        print("Info; PV Estimate 90% (30 mins, 7 days) / kWh", ";", timestamp, ";",
            self.pv_est90_30[0:47], self.pv_est90_day[0:6])

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
        except resp.exceptions.RequestException as error:
            print("Warning: Problem obtaining CO2 intensity:", error)
            return
        if resp.status_code != 200:
            print("Invalid response:", resp.status_code)
            return

        if len(resp.content) < 50:
            print("Warning: Carbon intensity data missing/short")
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

        except Exception as error:
            print("Warning: Problem calculating CO2 intensity trend:", error)

        self.co2_high = co2_intens_far > 1.3 * co2_intens_near or \
            co2_intens_far > stgs.CarbonIntensity.Threshold and \
            co2_intens_far > co2_intens_near

        if DEBUG_MODE:
            print(co2_intens_raw)
            print("Info; CO2 Intensity:", self.co2_intensity, round(co2_intens_near),\
                round(co2_intens_far), self.co2_high)

    def check_sr_ss(self) -> bool:
        """Adjust sunrise and sunset to reflect actual conditions"""

        new_virt_sr_ss = False
        pwr_threshold = stgs.PVData.PwrThreshold
        if TIME_NOW_MINS_VAR < time_to_mins(env_obj.virt_sr_time):  # Gen started?
            if (ge.sys_status[1]['solar']['power'] < pwr_threshold <
                ge.sys_status[0]['solar']['power']):
                new_virt_sr_ss = True
                self.virt_sr_time = ge.sys_status[0]['time'][11:]
                print('Info; VSunrise/set (Sunrise detected) VSR:',
                      env_obj.virt_sr_time, "VSS:", env_obj.virt_ss_time)
        elif TIME_NOW_MINS_VAR > 900:  # It's afternoon, gen ended?
            if (ge.sys_status[0]['solar']['power'] < pwr_threshold and
                (pwr_threshold < ge.sys_status[1]['solar']['power'] or LOOP_COUNTER_VAR < 10)):
                new_virt_sr_ss = True
                self.virt_ss_time = ge.sys_status[0]['time'][11:]
                print('Info; VSunrise/set (Sunset detected) VSR:',
                      env_obj.virt_sr_time, "VSS:", env_obj.virt_ss_time)
            elif (ge.sys_status[0]['solar']['power'] > pwr_threshold >
                ge.sys_status[1]['solar']['power']):
                # False alarm - sun back up
                new_virt_sr_ss = True
                self.virt_ss_time = env_obj.ss_time
                print('Info; VSunrise/set (False alarm) VSR:',
                      env_obj.virt_sr_time, "VSS:", env_obj.virt_ss_time)
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
        except resp.exceptions.RequestException as error:
            print(error)
            return
        if resp.status_code != 200:
            print("Invalid response:", resp.status_code)
            return

        if len(resp.content) < 50:
            print("Warning: Weather data missing/short")
            print(resp.content)
            return

        current_weather = json.loads(resp.content.decode('utf-8'))
        if DEBUG_MODE:
            print(current_weather)
        self.current_weather = current_weather

        self.temp_deg_c = round(current_weather['current']['temp'] - 273, 1)
        self.weather_symbol = current_weather['current']['weather'][0]['id']

# End of EnvObj() class definition

def time_to_mins(time_in_hrs: str) -> int:
    """Convert times from HH:MM format to mins after midnight."""

    time_in_mins = 60 * int(time_in_hrs[0:2]) + int(time_in_hrs[3:5])
    return time_in_mins

#  End of time_to_mins()

def time_to_hrs(time_in: int) -> str:
    """Convert times from mins after midnight format to HH:MM."""

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

    # Delay to avoid dropped commands at MiHome server and interference between base stations
    time.sleep(5)

    try:
        resp = requests.put(url, auth=(user_id, api_key), json=payload, timeout=5)
        resp.raise_for_status()
    except resp.exceptions.RequestException as error:
        print(error)
        return False
    if resp.status_code != 200:
        print("Invalid response:", resp.status_code)
        return False

    parsed = json.loads(resp.content.decode('utf-8'))
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

    if not TEST_MODE and stgs.PVOutput.enable:
        try:
            resp = requests.get(url, params=payload, timeout=10)
            resp.raise_for_status()
        except resp.exceptions.RequestException as error:
            print("Warning; PVOutput Write Error ", LONG_TIME_NOW_VAR)
            print(error)
            print()
            return()
        if resp.status_code != 200:
            print("Invalid response:", resp.status_code)
            return

    print("Data; Write to pvoutput.org;", post_date,";", post_time, ";", part_payload)
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

    print("Info; PALM... PV Automated Load Manager Version:", PALM_VERSION)
    print()
    print("Command line options (only one can be used):")
    print("-t | --test  | test mode (normal speed, no external server writes)")
    print("-f | --fast  | test mode (12x speed, no external server writes)")
    print("-d | --debug | debug mode, extra verbose")
    print()

    # Parse any command-line arguments
    TEST_MODE: bool = False
    DEBUG_MODE: bool = False
    FAST_MODE: bool = False
    if len(sys.argv) > 1:
        if str(sys.argv[1]) in ["-f", "--fast"]:
            TEST_MODE = True
            DEBUG_MODE = True
            FAST_MODE = True
            print("Info; Running in fast test mode... 5 sec loop time, no external server writes")
        elif str(sys.argv[1]) in ["-t", "--test"]:
            TEST_MODE = True
            DEBUG_MODE = True
            print("Info; Running in test mode... no external server writes")
        elif str(sys.argv[1]) in ["-d", "--debug"]:
            DEBUG_MODE = True
            print("Info; Running in debug mode, extra verbose")
    print()
    sys.stdout.flush()

    LOOP_COUNTER_VAR: int = 0  # 1 minute minor frame. "0" = initialise
    PVO_TSTAMP_VAR: int = 0  # Records value of LOOP_COUNTER_VAR when PV data last written

    while True:  # Main Loop
        # Current time definitions
        LONG_TIME_NOW_VAR: str = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime())
        MONTH_VAR: str = LONG_TIME_NOW_VAR[3:5]
        TIME_NOW_VAR: str = LONG_TIME_NOW_VAR[11:]
        TIME_NOW_MINS_VAR: int = time_to_mins(TIME_NOW_VAR)

        if LOOP_COUNTER_VAR == 0:  # Initialise
            # GivEnergy power object
            ge: GivEnergyObj = GivEnergyObj()
            time.sleep(10)
            ge.get_load_hist()

            # Solcast PV prediction object
            solcast: SolcastObj = SolcastObj()
            if not FAST_MODE:
                solcast.update()

            # Scheduler
            scheduler: SchedulerObj = SchedulerObj()
            scheduler.update()
            scheduler.display()

            # Misc environmental data: weather, CO2, etc
            CO2_USAGE_VAR: int = 200
            env_obj: EnvObj = EnvObj()
            env_obj.update_weather_curr()
            env_obj.update_co2()

            # Create an object for each load
            load_obj: List[LoadObj] = []
            load_payload: List[str] = []
            NUM_LOADS: int = len(stgs.LOAD_CONFIG['LoadPriorityOrder'])
            for LOAD_INDEX_VAR in range(NUM_LOADS):
                load_payload = stgs.LOAD_CONFIG[stgs.LOAD_CONFIG \
                    ['LoadPriorityOrder'][LOAD_INDEX_VAR]]
                load_obj.append(LoadObj(LOAD_INDEX_VAR, load_payload))

        else:  # Schedule activities at specific intervals... forever!

            # 5 minutes before off-peak start for next day's forecast
            if TIME_NOW_MINS_VAR == time_to_mins(stgs.GE.start_time) - 5:
                try:
                    solcast.update()
                    scheduler.update()
                except Exception:
                    print("Warning; Solcast download failure")

            # 2 minutes before off-peak start for setting overnight battery charging target
            if (TEST_MODE and LOOP_COUNTER_VAR == 2) or \
                (TIME_NOW_MINS_VAR == time_to_mins(stgs.GE.start_time) - 2):
                # compute & set SoC target
                try:
                    scheduler.update()
                    print("Info; 10% forecast...")
                    scheduler.compute_tgt_soc(solcast, 10, False)
                    print("Info; 50% forecast...")
                    scheduler.compute_tgt_soc(solcast, 50, False)
                    print("Info; 90% forecast...")
                    scheduler.compute_tgt_soc(solcast, 90, False)
                    print("Info; 35% weighted forecast...")
                    scheduler.compute_tgt_soc(solcast, 35, True)
                    scheduler.display()
                except Exception:
                    print("Warning; unable to set SoC")

            # Half-hourly settings
            if (TEST_MODE and LOOP_COUNTER_VAR == 4) or TIME_NOW_MINS_VAR % 30 == 0:
                index = 0
                while index < 48:
                    if scheduler.ttime[index] == TIME_NOW_VAR[0:5]:
                        mode_cmd = scheduler.inv_mode[index]
                        mode_res = scheduler.reserve[index]
                        mode_soc = scheduler.inv_soc[index]
                        ge.set_mode(mode_cmd, mode_res, mode_soc)
                        break
                    index += 1

            # Update weather & carbon intensity every 15 mins as background tasks
            if not TEST_MODE and LOOP_COUNTER_VAR % 15 == 14:
                do_get_carbon_intensity = threading.Thread(target=env_obj.update_co2())
                do_get_carbon_intensity.daemon = True
                do_get_carbon_intensity.start()

                do_get_weather = threading.Thread(target=env_obj.update_weather_curr())
                do_get_weather.daemon = True
                do_get_weather.start()

            #  Refresh utilisation data from GivEnergy server every minute
            ge.get_latest_usage()
            CO2_USAGE_VAR = int(env_obj.co2_intensity * ge.grid_power / 1000)

            #  Turn loads on or off every minute
            do_balance_loads = threading.Thread(target=balance_loads())
            do_balance_loads.daemon = True
            do_balance_loads.start()

            # Publish data to PVOutput.org every 5 minutes (or 5 cycles as a catch-all)
            if TEST_MODE or TIME_NOW_MINS_VAR % 5 == 3 or LOOP_COUNTER_VAR > PVO_TSTAMP_VAR + 4:
                PVO_TSTAMP_VAR = LOOP_COUNTER_VAR
                do_put_pv_output = threading.Thread(target=put_pv_output)
                do_put_pv_output.daemon = True
                do_put_pv_output.start()

        LOOP_COUNTER_VAR += 1
        if TIME_NOW_MINS_VAR == 0:  # Reset frame counter every 24 hours
            ge.pv_energy = 0  # Avoids carry-over issues with PVOutput
            ge.grid_energy = 0
            env_obj.reset_sr_ss()
            LOOP_COUNTER_VAR = 1

        if FAST_MODE:  # Wait 5 seconds
            print("######## LOOP COUNT", LOOP_COUNTER_VAR)
            time.sleep(5)
        else:  # Sync to minute rollover on system clock
            CURRENT_MINUTE = int(time.strftime("%M", time.localtime()))
            while int(time.strftime("%M", time.localtime())) == CURRENT_MINUTE:
                time.sleep(10)

        sys.stdout.flush()
# End of main
