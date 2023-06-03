#!/usr/bin/env python3
"""PALM - PV Active Load Manager - separate class file for GivEnergyObj."""

import settings as stgs
import requests

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

        if DEBUG_MODE:
            print("Valid inverter commands:")
            for line in self.cmd_list:
                print(line['id'], " - ", line['name'])
            print("")

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
                        except Exception:
                            print("Error reading GivEnergy system status ", T_NOW_VAR)
                            print(resp.content)
                            self.sys_status[index] = self.sys_status[index + 1]
                if LOOP_COUNTER_VAR == 0:  # Pack array on startup
                    index = 1
                    while index < 5:
                        self.sys_status[index] = self.sys_status[0]
                        index += 1
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
                        except Exception:
                            print("Error reading GivEnergy meter status ", T_NOW_VAR)
                            print(resp.content)
                            self.meter_status[index] = self.meter_status[index + 1]
                if LOOP_COUNTER_VAR == 0:  # Pack array on startup
                    index = 1
                    while index < 5:
                        self.meter_status[index] = self.meter_status[0]
                        index += 1

                self.pv_energy = int(self.meter_status[0]['today']['solar'] * 1000)

                # Daily grid energy must be >=0 for PVOutput.org (battery charge >= midnight value)
                self.grid_energy = max(int(self.meter_status[0]['today']['consumption'] * 1000), 0)

    def get_load_hist(self):
        """Download historical consumption data from GivEnergy and pack array for next SoC calc"""

        day_delta = 0 if (T_NOW_MINS_VAR > 1430) else 1  # Use latest full day
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
                except Exception:
                    break
                if counter == 0:
                    self.base_load[counter] = round(current_energy, 1)
                else:
                    self.base_load[counter] = round(current_energy - prev_energy, 1)
                counter += 1
                prev_energy = current_energy
                index += 12
            print("Info; Load Calc Summary:", current_energy, self.base_load)

    def set_mode(self, cmd: str, *arg: str):
        """Configures inverter operating mode"""

        def set_inverter_register(register: str, value: str):
            """Exactly as it says"""

            # Removed check as it can throw errors if network down on startup
            cmd_name = ""
            for line in self.cmd_list:
                if line['id'] == int(register):
                    cmd_name = line['name']
                    break

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
            if not TEST_MODE:
                try:
                    resp = requests.request('POST', url, headers=headers, json=payload)
                except requests.exceptions.RequestException as error:
                    print(error)
                    return
            print("Info; Setting Register ", register, " (", cmd_name, ") to ", value, ", \
                    Response:", resp, sep='')

        if cmd == "set_soc":  # Sets target SoC to value
            set_inverter_register("77", arg[0])
            if stgs.GE.start_time != "":
                set_inverter_register("64", stgs.GE.start_time)
            if stgs.GE.end_time != "":
                set_inverter_register("65", stgs.GE.end_time)

        elif cmd == "set_soc_winter":  # Restore default overnight charge params
            set_inverter_register("77", "100")
            if stgs.GE.start_time != "":
                set_inverter_register("64", stgs.GE.start_time)
            if stgs.GE.end_time_winter != "":
                set_inverter_register("65", stgs.GE.end_time_winter)

        elif cmd == "charge_now":
            set_inverter_register("77", "100")
            set_inverter_register("64", "00:30")
            set_inverter_register("65", "23:59")

        elif cmd == "pause":
            set_inverter_register("72", "0")
            set_inverter_register("73", "0")

        elif cmd == "resume":
            set_inverter_register("72", "3000")
            set_inverter_register("73", "3000")

        else:
            print("error: unknown inverter command:", cmd)

    def compute_tgt_soc(self, gen_fcast, weight: int, commit: bool):
        """Compute tomorrow's overnight SoC target"""

        # Winter months = 100%
        if MNTH_VAR in stgs.GE.winter and commit:  # No need for sums...
            print("info; winter month, SoC set to 100%")
            self.set_mode("set_soc_winter")
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

        tgt_soc = 100
        if gen_fcast.pv_est50_day[0] > 0:  # Quick check for valid generation data

            # The clever bit:
            # Start with a battery at 100%. For each hour of the coming day, calculate the
            # battery charge based on forecast generation and historical usage. Capture values for
            # maximum charge and also the minimum charge value at any time before the maximum.

            batt_max_charge: float = stgs.GE.batt_max_charge
            batt_charge: float = [0] * 24
            batt_charge[0] = batt_max_charge
            max_charge = 0
            min_charge = batt_max_charge

            print("")
            print("{:<20} {:>10} {:>10} {:>10}  {:>10} {:>10}".format("Info; SoC Calcs;",
                "Hour", "Charge", "Cons", "Gen", "SoC"))

            if stgs.GE.end_time != "":
                end_charge_period = int(stgs.GE.end_time[0:2])
            else:
                end_charge_period = 4

            index = 0
            est_gen = 0
            while index < 24:
                if index <= end_charge_period:  # Battery is in AC Charge mode
                    total_load = 0
                else:
                    total_load = ge.base_load[index]

                if index > 0:
                    # Generation is in GMT, add an hour for BST
                    est_gen = (gen_fcast.pv_est10_hrly[index - 1] * wgt_10 +
                        gen_fcast.pv_est50_hrly[index - 1] * wgt_50 +
                        gen_fcast.pv_est90_hrly[index - 1] * wgt_90) / (wgt_10 + wgt_50 + wgt_90)

                    batt_charge[index] = (batt_charge[index - 1] +
                        max(-1 * stgs.GE.charge_rate,
                            min(stgs.GE.charge_rate, est_gen - total_load)))
                    # Capture min charge on lowest down-slope before charge exceeds 100% append
                    # max xharge if on an up slope after overnight charge
                    if (batt_charge[index] <= batt_charge[index - 1] and
                        max_charge < batt_max_charge):
                        min_charge = min(min_charge, batt_charge[index])
                    elif index > end_charge_period:  # Charging after overnight boost
                        max_charge = max(max_charge, batt_charge[index])

                print("{:<20} {:>10} {:>10} {:>10}  {:>10} {:>10}".format("Info; SoC Calc;",
                    t_to_hrs(index * 60), round(batt_charge[index], 2), round(total_load, 2),
                    round(est_gen, 2), int(100 * batt_charge[index] / batt_max_charge)))

                index += 1

            max_charge_pcnt = int(100 * max_charge / batt_max_charge)
            min_charge_pcnt = int(100 * min_charge / batt_max_charge)

            # low_soc is the minimum SoC target. Provide more buffer capacity in shoulder months
            if MNTH_VAR in stgs.GE.shoulder:
                low_soc = stgs.GE.max_soc_target
            else:
                low_soc = stgs.GE.min_soc_target

            # The really clever bit is just two lines:
            # Reduce the target SoC to the greater of:
            #     The surplus above 100% for max_charge_pcnt
            #     The spare capacity in the battery before the maximum charge point
            #     The preset minimum value
            # Range check the resulting value
            tgt_soc = max(200 - max_charge_pcnt, 100 - min_charge_pcnt, low_soc)
            tgt_soc = int(min(tgt_soc, 100))  # Limit range to 100%

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

        print("Info; SoC Summary; ", LONG_T_NOW_VAR, "; Tom Fcast Gen (kWh); ",
            gen_fcast.pv_est10_day[0], ";", gen_fcast.pv_est50_day[0], ";",
            gen_fcast.pv_est90_day[0], "; SoC Target (%); ", tgt_soc,
            "; Today Gen (kWh); ", round(self.pv_energy) / 1000, 2)

        if commit:
            self.set_mode("set_soc", str(tgt_soc))
            self.tgt_soc = tgt_soc

# End of GivEnergyObj() class definitionabs

DEBUG_MODE: bool = False
TEST_MODE: bool = False

