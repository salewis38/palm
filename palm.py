#!/usr/bin/env python3
"""PALM - PV Active Load Manager."""

import sys
import time
import csv
import threading
import json
from typing import List
from urllib.parse import urlencode
import logging
import requests
from palm_utils import GivEnergyObj, SolcastObj, t_to_mins, t_to_hrs
import palm_settings as stgs

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
# v1.1.0    06/Aug/23 Split out generic functions as palm_utils.py
# v1.1.1    19/Nov/23 Updated to Shelly Gen 2 switch, improved readability
# v1.1.2    03/Dec/23 Added Shelly switch to load balancing, updated Events logic for robustness
# v1.1.3    01/Jan/24 Added routine to update PVOutput daily stats with IO Smart Charge periods
# v1.1.3a   28/Jan/24 Revise PVOutput write timing to improve alignment of inverter and local data
# V1.1.3b   28/Mar/24 Remove manual hold function, fixed in latest AC3 firmware. Remove v3 from PVO payload

PALM_VERSION = "v1.1.3b"
# -*- coding: utf-8 -*-
# pylint: disable=logging-not-lazy
# pylint: disable=consider-using-f-string

class LoadObj:
    """Class for each controlled load."""

    def __init__(self, load_i: int, l_payload):

        # Pull in data from Load_Config
        self.base_priority: int = load_i + 1  # Sets the initial priority for load
        self.load_record = l_payload  # Pulls in load configuration details
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
            if not stgs.pg.test_mode:
                if self.load_record["DeviceType"] == "MiHome":
                    set_mihome_switch(self.load_record["DeviceID"], True)
                elif self.load_record["DeviceType"] == "Shelly":
                    set_shelly_switch(self.load_record["DeviceID"], True)
            logger.info("Device ON event: "+ str(self.load_record["DeviceID"])+ " ETI: "+
                        str(self.eti)+ " Name: "+ str(self.load_record["DeviceName"]))
            self.curr_state = "ON"
            self.est_power = self.load_record["PwrLoad"] - self.load_record["Hysteresis"]
            self.eti += 1
            self.ontime = 0  # Reset ontime whenever load toggles state
            return self.est_power

        if cmd == "OFF" and self.prev_state == "ON":
            if not stgs.pg.test_mode:
                if self.load_record["DeviceType"] == "MiHome":
                    set_mihome_switch(self.load_record["DeviceID"], False)
                elif self.load_record["DeviceType"] == "Shelly":
                    set_shelly_switch(self.load_record["DeviceID"], False)
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
        if stgs.pg.t_now_mins == 0:
            self.eti = 0

        if self.curr_state == "ON":
            self.ontime += 1  # Count up when load is on
            self.eti += 1
        else:
            self.ontime -= 1  # Count down when load is off

        # Does schedule sit within a single day?
        if self.finish_time_mins >= self.early_start_mins:
            valid_time = self.early_start_mins <= stgs.pg.t_now_mins < self.finish_time_mins
        else:
            valid_time = (self.early_start_mins <= stgs.pg.t_now_mins or
                stgs.pg.t_now_mins < self.finish_time_mins)

        # Force a start if load has timed-out with run time below its daily target
        late_start_active = (self.late_start_mins < stgs.pg.t_now_mins and
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
            inverter.soc < self.load_record["MinBattSoc"]):
            self.priority = 99
        elif self.eti > self.load_record["MinDailyTarget"]:
            self.priority = int(self.base_priority) + 50
        else:
            self.priority = int(self.base_priority)
        self.priority_change = self.priority != old_priority
# End of LoadObj() class definition


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
            resp = requests.get(url, params={}, headers=headers, timeout=10)
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
        if stgs.pg.t_now_mins < t_to_mins(env_obj.virt_sr_time):  # Gen started?
            if (inverter.sys_status[1]['solar']['power'] < pwr_threshold <
                inverter.sys_status[0]['solar']['power']):
                new_virt_sr_ss = True
                self.virt_sr_time = inverter.sys_status[0]['time'][11:]
                logger.info("VSunrise/set (Sunrise detected) VSR: " +
                      str(env_obj.virt_sr_time)+ " VSS: "+ str(env_obj.virt_ss_time))
        elif stgs.pg.t_now_mins > 900:  # It's afternoon, gen ended?
            if (inverter.sys_status[0]['solar']['power'] < pwr_threshold and
                (pwr_threshold < inverter.sys_status[1]['solar']['power'] or stgs.pg.loop_counter < 10)):
                new_virt_sr_ss = True
                self.virt_ss_time = inverter.sys_status[0]['time'][11:]
                logger.info("VSunrise/set (Sunset detected) VSR: " +
                      str(env_obj.virt_sr_time)+ " VSS: "+ str(env_obj.virt_ss_time))
            elif (inverter.sys_status[0]['solar']['power'] > 2 * pwr_threshold >
                inverter.sys_status[1]['solar']['power']):
                # False alarm - sun back up (added hysteresis to threshold)
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
        logger.error(str(error))
        return False

    parsed = json.loads(resp.content.decode('utf-8'))
    if parsed['status'] == "success":
        return True
    logger.warning("Failure..."+ url + device_id)
    return False

#  End of set_mihome_switch()


def set_shelly_switch(base_url: str, turn_on: bool) -> bool:
    """Operates a Shelly Plus 1 (Gen 2) switch on/off."""

    if turn_on:
        sw_cmd = "on"
    else:
        sw_cmd = "off"

    url:str = base_url + "relay/0/?turn=" + sw_cmd

    try:
        resp = requests.put(url, timeout=5)
        resp.raise_for_status()
    except requests.exceptions.RequestException as error:
        logger.error(str(error))
        return False

    return True

#  End of set_shelly_switch()


def read_shelly_switch(base_url: str) -> str:
    """Reads Shelly Plus 1 (Gen 2) switch value"""

    url:str = str(base_url) + "rpc/Input.GetStatus?id=0"

    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
    except requests.exceptions.RequestException as error:
        logger.error("Missing response from Shelly EM: "+ str(error))
        return "Error"

    parsed = json.loads(resp.content.decode('utf-8'))
    logger.debug(str(parsed))

    if parsed['state'] is True:
        return "On-stat"
    return "Off"

# End of read_shelly_switch()


class EVObj:
    """Reports status and stores instantaneous measured power to EV using Shelly EM"""

    def __init__(self):
        self.power: int = 0
        self.power_last: int = 0
        self.active_now: bool = False
        self.active_last: bool = False
        self.active: bool = False
        self.confirmed_active: bool = False

    def charging(self) -> bool:
        """Polls Shelly EM and updates status"""

        url:str = str(stgs.Shelly.em0_url)
        if url == "":
            return False

        try:
            resp = requests.put(url, timeout=5)
            resp.raise_for_status()
        except requests.exceptions.RequestException as error:
            logger.error("Missing response from Shelly EM"+ str(error))
            return False

        parsed = json.loads(resp.content.decode('utf-8'))
        logger.debug(str(parsed))

        if parsed['is_valid'] is True:
            self.power_last = self.power
            self.power = int(parsed['power'])
            self.active_last = self.active_now
            self.active_now = self.power > 500
            if self.active_now is True and self.active_last is False:  # Edge detect
                logger.warning("EV charging detected, power = "+ str(parsed['power']))
        self.active = self.active_last and self.active_now
        return self.active
    # End of charging()

# End of EVObj


def put_pv_output():
    """Upload generation/consumption data to PVOutput.org."""

    url = stgs.PVOutput.url + "addstatus.jsp"
    key = stgs.PVOutput.key
    sid = stgs.PVOutput.sid

    # Backdate measurements by 60 seconds
    post_date = time.strftime("%Y%m%d", time.localtime(time.time() - 60))
    post_time = time.strftime("%H:%M", time.localtime(time.time() - 60))

    batt_power_out = inverter.batt_power if inverter.batt_power > 0 else 0
    batt_power_in = -1 * inverter.batt_power if inverter.batt_power < 0 else 0
    total_cons = inverter.consumption - inverter.batt_power
    load_pwr = total_cons if total_cons > 0 else 0

    if stgs.pg.test_mode is True:
        print("### TEST Inverter read time: ", inverter.read_time_mins)

    payload = {
        "t"   : post_time,
        "key" : key,
        "sid" : sid,
        "d"   : post_date
    }

    # v3 removed from payload to avoid import/export creep during the day
    part_payload = {
        "v1"  : inverter.pv_energy,
        "v2"  : inverter.pv_power,
        "v4"  : load_pwr,
        "v5"  : env_obj.temp_deg_c,
        "v6"  : inverter.line_voltage,
        "v7"  : ev.power_last,
        "v8"  : batt_power_out,
        "v9"  : env_obj.co2_intensity,
        "v10" : CO2_USAGE_VAR,
        "v11" : batt_power_in,
        "v12" : inverter.soc
    }

    payload.update(part_payload)  # Concatenate the data, don't escape ":"
    payload = urlencode(payload, doseq=True, quote_via=lambda x,y,z,w: x)

    time.sleep(2)  # PVOutput has a 1 second rate limit. Avoid any clashes

    if not stgs.pg.test_mode:
        try:
            resp = requests.get(url, params=payload, timeout=10)
            resp.raise_for_status()
        except requests.exceptions.RequestException as error:
            logger.warning("PVOutput Write Error "+ stgs.pg.long_t_now)
            logger.warning(error)
            return()

    logger.info("Data; Write to pvoutput.org; "+ post_date+"; "+ post_time+ "; "+ str(part_payload))
    return()

#  End of put_pv_output()


def resummarise_pv_output(post_date: str):
    """Calculate and upload summary of generation/consumption data to PVOutput.org."""

    # Step 1. Download existing daily summary data
    url = stgs.PVOutput.url + "getstatistic.jsp"
    key = stgs.PVOutput.key
    sid = stgs.PVOutput.sid

    payload = {
        "c"   : 1,
        "key" : key,
        "sid" : sid,
        "df"  : post_date,
        "dt"  : post_date
    }

    payload = urlencode(payload, doseq=True, quote_via=lambda x,y,z,w: x)

    try:
        resp = requests.get(url, params=payload, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as error:
        logger.warning("PVOutput Read Error "+ stgs.pg.long_t_now)
        logger.warning(error)
        return

    stats = list(resp.content.decode('utf-8').split(','))
    e_gen = int(stats[0])
    e_pk = int(stats[12])
    e_off_pk = int(stats[13])
    e_shldr = int(stats[14])

    logger.warning("Stats:"+ str(e_gen)+ str(e_pk)+ str(e_off_pk)+ str(e_shldr))

    if e_shldr > 0:
        logger.warning("Shoulder values already computed for "+ str(post_date)+ ". Exiting")
        return

    # Step 2. Download 5-minute usage data for analysis
    url = stgs.PVOutput.url + "getstatus.jsp"
    key = stgs.PVOutput.key
    sid = stgs.PVOutput.sid

    payload = {
        "h"   : 1,
        "limit": 288,
        "ext" : 1,
        "key" : key,
        "sid" : sid,
        "d"   : post_date
    }

    payload = urlencode(payload, doseq=True, quote_via=lambda x,y,z,w: x)

    try:
        resp = requests.get(url, params=payload, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as error:
        logger.warning("PVOutput Read Error "+ stgs.pg.long_t_now)
        logger.warning(error)
        return

    pv_data = resp.content.decode('utf-8')

    headings = "Date,Time,EGen,EEff,PInst,PAvg,NormOp,ECons,PCons,Temp,Volts,v7 - PEV,v8 - PBattOut,v9 - CO2 Intens,v10 CO2 Usage,v11 - PBattIn,v12 - SoC;"

    measured_data = headings + pv_data

    d_time:str = [0] * 300
    d_pwr_ev:int = [0] * 300
    d_pwr_import:int = [0] * 300

    csv_data = csv.DictReader(measured_data.split(';'))

    # Step 3. Extract fields from received data. Data is received in reverse order
    i = 0
    for row in csv_data:
        d_time[i] = str(row['Time'])
        d_pwr_ev[i] = int(float(row['v7 - PEV']))
        d_pwr_import[i] = int(row['ECons']) - int(row['EGen'])
        i += 1

    # Step 4. Work back through data to identify peak periods where EV is active,
    # and sum energy imported in periods
    e_shldr = 0
    while i > 4:
        t_row_mins = t_to_mins(d_time[i])
        if t_row_mins % 30 == 0:
            if 330 < t_row_mins < 1410 and sum(d_pwr_ev[i:i+5]) > 1000:  # Peak hours, EV active
                e_shldr += d_pwr_import[i-5] - d_pwr_import[i]
        i -= 1

    if e_shldr == 0:
        logger.warning("No shoulder generation identified")
        return

    e_pk_new = e_pk - e_shldr

    logger.warning("Daily adjustments for "+ str(post_date)+ ":")
    logger.warning("Off Peak:"+ str(e_off_pk))
    logger.warning("Peak: "+ str(e_pk)+ " now: "+ str(e_pk_new))
    logger.warning("Shoulder: "+ str(e_shldr))

    # Step 5. Upload revised values to PVOutput.org
    url = stgs.PVOutput.url + "addoutput.jsp"
    key = stgs.PVOutput.key
    sid = stgs.PVOutput.sid

    payload = {
        "sid" : sid,
        "key" : key
    }

    part_payload = {
        "d"   : post_date,
        "g"   : e_gen,
        "ip"  : e_pk_new,
        "io"  : e_off_pk,
        "is"  : e_shldr
    }

    payload.update(part_payload)  # Concatenate the data, don't escape ":"
    payload = urlencode(payload, doseq=True, quote_via=lambda x,y,z,w: x)

    time.sleep(2)  # PVOutput has a 1 second rate limit. Avoid any clashes

    if not stgs.pg.test_mode:
        try:
            resp = requests.get(url, params=payload, timeout=10)
            resp.raise_for_status()
        except requests.exceptions.RequestException as error:
            logger.warning("PVOutput Write Error "+ stgs.pg.long_t_now)
            logger.warning(error)
            logger.warning(resp.content)
            return

    logger.info("Data; Write to pvoutput.org; "+ str(part_payload))
    return

# End of resummarise_pv_output


def balance_loads():
    """control loads, based on schedule, generation, temp, etc."""

    new_virt_sr_ss = env_obj.check_sr_ss()

    # Running total of available power. Positive means export
    net_usage_est = inverter.pv_power - inverter.consumption

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
                net_usage_est < 0 and inverter.soc > 98):  # Capacity exists, turn on load

                net_usage_est += unique_load.toggle("ON")

    # Third pass: Turn off loads to rebalance power, lowest priority first
    for priority in range(90, 1, -1):
        for unique_load in load_obj:
            if (unique_load.priority == priority and
                unique_load.curr_state == "ON" and
                (net_usage_est > 0 or inverter.soc < 95)):  # Turn off load

                net_usage_est -= unique_load.toggle("OFF")

#  End of balance_loads()


class EventsObj:
    """Definitions used to trigger events each minute in scheduler. Less messy this way """

    def __init__(self):
        self.shoulder: bool = False
        self.winter: bool = False
        self.off_pk: bool = False
        self.off_pk_start: bool = False
        self.off_pk_ending: bool = False
        self.off_pk_end: bool = False
        self.pm_boost_start: bool = False
        self.pm_boost_end: bool = False
        self.update_pv_fcast: bool = False
        self.update_soc: bool = False
        self.resumm_pvoutput: bool = False
        self.update_carbon_intensity: bool = False
        self.update_weather: bool = False

    def update(self):
        """Values are updated every minute for use by logic in main code loop"""
        t_now = stgs.pg.t_now_mins
        t_plus_hr = t_now + 60 % 1440

        self.shoulder = stgs.pg.month in stgs.GE.shoulder
        self.winter = stgs.pg.month in stgs.GE.winter

        if stgs.GE.start_time != "" and stgs.GE.end_time != "":
            # Is current time is within off-peak window? Needs to consider spanning midnight
            self.off_pk_start = t_to_mins(stgs.GE.start_time) == t_now
            self.off_pk = t_to_mins(stgs.GE.start_time) < t_now < t_to_mins(stgs.GE.end_time) or \
                t_now > t_to_mins(stgs.GE.start_time) > t_to_mins(stgs.GE.end_time) or \
                t_to_mins(stgs.GE.start_time) > t_to_mins(stgs.GE.end_time) > t_now

            # 5 minutes before off-peak start and 1hr before off-peak ends
            self.update_pv_fcast = \
                ((stgs.pg.test_mode or stgs.pg.once_mode) and stgs.pg.loop_counter == 1) or \
                t_now == (t_to_mins(stgs.GE.start_time) + 1435) % 1440 or \
                t_now == (t_to_mins(stgs.GE.end_time) + 1375) % 1440

            # 2 minutes before off-peak start for setting overnight battery charging target
            # Repeat 60 mins before end of off-peak in case of Solcast fine-tuning
            self.update_soc = \
                ((stgs.pg.test_mode or stgs.pg.once_mode) and stgs.pg.loop_counter == 2) or \
                t_now == (t_to_mins(stgs.GE.start_time) + 1438) % 1440 or \
                t_now == (t_to_mins(stgs.GE.end_time) + 1380) % 1440

        if stgs.GE.end_time != "" and stgs.GE.end_time_winter != "":
            # Flag 1 hour before end of off-peak
            self.off_pk_ending = self.winter is True and \
                t_plus_hr == t_to_mins(stgs.GE.end_time_winter) or \
                self.winter is False and t_plus_hr == t_to_mins(stgs.GE.end_time)
            # Flag at end of off-peak
            self.off_pk_end = \
                self.winter is True and t_now == t_to_mins(stgs.GE.end_time_winter) or \
                self.winter is False and t_now == t_to_mins(stgs.GE.end_time)

        # Afternoon boost options
        if stgs.GE.boost_start != "" and stgs.GE.boost_finish != "":
            self.pm_boost_start = self.winter is True or self.shoulder is True and \
                t_now == t_to_mins(stgs.GE.boost_start)
            self.pm_boost_end = self.winter is True or self.shoulder is True and \
                t_now == t_to_mins(stgs.GE.boost_finish)

        # Summarise daily data at PVOutput.org
        self.resumm_pvoutput = stgs.PVOutput.enable is True and \
                    stgs.Shelly.em0_url != "" and \
                    (stgs.pg.test_mode and stgs.pg.loop_counter == 4 or \
                    stgs.pg.t_now_mins == 1438)

        # Update carbon intensity and weather every 15 mins
        self.update_carbon_intensity = \
            stgs.CarbonIntensity.enable is True and stgs.pg.loop_counter % 15 == 14
        self.update_weather = \
            stgs.OpenWeatherMap.enable is True and stgs.pg.loop_counter % 15 == 14

# End of EventsObj

if __name__ == '__main__':

    # Parse any command-line arguments

    MESSAGE = ""
    if len(sys.argv) > 1:
        if str(sys.argv[1]) in ["-t", "--test"]:
            stgs.pg.test_mode = True
            stgs.pg.debug_mode = True
            MESSAGE = "Running in test mode... 5 sec loop time, no external server writes"
        elif str(sys.argv[1]) in ["-d", "--debug"]:
            stgs.pg.debug_mode = True
            MESSAGE = "Running in debug mode, extra verbose"
        elif str(sys.argv[1]) in ["-o", "--once"]:
            stgs.pg.once_mode = True
            MESSAGE = "Running in once mode, execute forecast and inverter SoC update, then exit"

    if stgs.pg.debug_mode:
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

    EV_ACTIVE_VAR: bool = False

    while True:  # Main Loop
        # Current time definitions
        stgs.pg.long_t_now: str = time.strftime("%d-%m-%Y %H:%M:%S %z", time.localtime())
        stgs.pg.month: str = stgs.pg.long_t_now[3:5]
        stgs.pg.t_now: str = stgs.pg.long_t_now[11:]
        stgs.pg.t_now_mins: int = t_to_mins(stgs.pg.t_now)

        if stgs.pg.loop_counter == 0:  # Initialise
            logger.critical("Initialising at: "+ stgs.pg.long_t_now)
            logger.critical("")
            sys.stdout.flush()

            # Initialise event semaphores
            events: EventsObj = EventsObj()

            # Object to capture EV charging status/current
            ev: EVObj = EVObj()

            # GivEnergy power object
            inverter: GivEnergyObj = GivEnergyObj()
            time.sleep(10)

            if stgs.pg.once_mode is False:
                if stgs.pg.month in stgs.GE.winter:
                    inverter.set_mode("set_soc_winter")
                else:
                    inverter.set_mode("set_soc")

            # Solcast PV prediction object
            pv_forecast: SolcastObj = SolcastObj()

            # Misc environmental data: weather, CO2, etc
            CO2_USAGE_VAR: int = 0
            env_obj: EnvObj = EnvObj()
            if stgs.CarbonIntensity.enable is True:
                env_obj.update_co2()
            if stgs.OpenWeatherMap.enable is True:
                env_obj.update_weather_curr()

            # Create an object for each load
            if stgs.pg.once_mode is False and stgs.LoadMgt.enable is True:
                load_obj: List[LoadObj] = []
                load_payload: List[str] = []
                NUM_LOADS: int = len(stgs.LOAD_CONFIG['LoadPriorityOrder'])
                for LOAD_INDEX_VAR in range(NUM_LOADS):
                    load_payload = stgs.LOAD_CONFIG[stgs.LOAD_CONFIG \
                        ['LoadPriorityOrder'][LOAD_INDEX_VAR]]
                    load_obj.append(LoadObj(LOAD_INDEX_VAR, load_payload))

        else:
            # Schedule activities at specific intervals
            events.update()

            if events.update_pv_fcast is True:
                try:
                    pv_forecast.update()
                except Exception:
                    logger.warning("Warning; Solcast download failure")

            if events.update_soc is True:
                # compute & set SoC target
                try:
                    inverter.get_load_hist()
                    logger.info("Forecast weighting: "+ str(stgs.Solcast.weight))
                    inverter.set_mode(inverter.compute_tgt_soc(pv_forecast, stgs.Solcast.weight, True))
                except Exception as e:
                    logger.error(str(type(e).__name__))
                    logger.error(str(e))
                    logger.error("Warning; unable to set SoC")

                # Send plot data to logfile in CSV format
                logger.info("SoC Chart Data - Start. Paste these lines into a spreadsheet for a plot of SoC")
                i = 0
                while i < 5:
                    logger.info(inverter.plot[i])
                    i += 1
                logger.info("SoC Chart Data - End")

                # if running in once mode, quit after inverter SoC update
                if stgs.pg.once_mode:
                    logger.info("PALM Once Mode complete. Exiting...")
                    sys.exit()

            if stgs.pg.once_mode is False:

                # Reset sunrise and sunset for next day
                env_obj.reset_sr_ss()

                # Poll car charger during additional Intelligent Octopus slots
                # If car is charging, either pause or charge inverter, depending on battery state
                # A Shelly switch also overrides the UFH thermostat in winter months to force on
                EV_ACTIVE_VAR = ev.charging()
                if events.off_pk is False:
                    if ev.confirmed_active is False and EV_ACTIVE_VAR is True:
                        ev.confirmed_active = True
                        if events.winter is True or events.shoulder is True:  # Fill battery
                            logger.info("EV charging: enabling battery boost at "+ \
                                stgs.pg.long_t_now)
                            inverter.set_mode("charge_now")
                            if env_obj.temp_deg_c < 15:  # Force heating on
                                set_shelly_switch(stgs.Shelly.sw1_url, True)
                        else:  # Put battery on hold during EV charging
                            logger.info("EV charging: pausing battery discharge at "+ \
                                stgs.pg.long_t_now)
                            inverter.set_mode("pause_discharge")
                    elif EV_ACTIVE_VAR is False and ev.confirmed_active is True:
                        if stgs.pg.t_now_mins % 30 < 3:  # Check at the end of every 30-minute metering period
                            logger.info("EV charging inactive, resuming ECO battery mode at "+ \
                                stgs.pg.long_t_now)
                            ev.confirmed_active = False
                            inverter.set_mode("resume")
                        if (events.off_pk_start or stgs.pg.t_now_mins % 30 < 3) and \
                            read_shelly_switch(stgs.Shelly.sw1_url) == "Off":  # Turn off heating as thermostat not active
                            set_shelly_switch(stgs.Shelly.sw1_url, False)

                # Afternoon battery boost in shoulder/winter months to load shift from peak period,
                # useful for Cosy Octopus, etc
                if events.pm_boost_start is True:
                    logger.info("Enabling afternoon battery boost")
                    inverter.tgt_soc = int(stgs.GE.max_soc_target)
                    inverter.set_mode("charge_now_soc")

                if events.pm_boost_end is True:
                    inverter.set_mode("set_soc")  # Set inverter for next timed charge period

                # Update carbon intensity every 15 mins as background task
                if events.update_carbon_intensity is True:
                    do_get_carbon_intensity = threading.Thread(target=env_obj.update_co2())
                    do_get_carbon_intensity.daemon = True
                    do_get_carbon_intensity.start()

                # Update weather every 15 mins as background task
                if events.update_weather is True:
                    do_get_weather = threading.Thread(target=env_obj.update_weather_curr())
                    do_get_weather.daemon = True
                    do_get_weather.start()

                #  Refresh utilisation data from GivEnergy server. Check every minute
                inverter.get_latest_data()
                CO2_USAGE_VAR = int(env_obj.co2_intensity * inverter.grid_power / 1000)

                if stgs.pg.t_now_mins > inverter.read_time_mins + 7:
                    logger.critical("Inverter last seen at: "+ t_to_hrs(inverter.read_time_mins))

                # Publish data to PVOutput.org
                if stgs.PVOutput.enable is True and \
                    (stgs.pg.test_mode or \
                    stgs.pg.t_now_mins == inverter.read_time_mins + 1 or \
                    stgs.pg.loop_counter > stgs.pg.pvo_tstamp + 5):

                    stgs.pg.pvo_tstamp = stgs.pg.loop_counter
                    if stgs.pg.t_now_mins < 6:  # Reset totals to avoid PVOutput carry-over issue
                        inverter.pv_energy = 0
                        inverter.grid_energy = 0
                    do_put_pv_output = threading.Thread(target=put_pv_output)
                    do_put_pv_output.daemon = True
                    do_put_pv_output.start()

                #  Turn loads on or off. Check every minute
                if stgs.LoadMgt.enable is True:
                    do_balance_loads = threading.Thread(target=balance_loads())
                    do_balance_loads.daemon = True
                    do_balance_loads.start()

                # Update PVOutput daily summary to reflect any IO Smart charging
                if events.resumm_pvoutput:
                    do_resumm_pv_output = threading.Thread(target=\
                        resummarise_pv_output(time.strftime("%Y%m%d", time.localtime())))
                    do_resumm_pv_output.daemon = True
                    do_resumm_pv_output.start()

        stgs.pg.loop_counter += 1

        if stgs.pg.t_now_mins == 0:  # Reset frame counter every 24 hours
            stgs.pg.loop_counter = 1

        if stgs.pg.test_mode or stgs.pg.once_mode:  # Wait 5 seconds
            time.sleep(5)
        else:  # Sync to minute rollover on system clock
            CURRENT_MINUTE = int(time.strftime("%M", time.localtime()))
            while int(time.strftime("%M", time.localtime())) == CURRENT_MINUTE:
                time.sleep(10)

        sys.stdout.flush()
# End of main
