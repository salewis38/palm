#!/usr/bin/env python3
"""PALM - PV Active Load Manager."""

import sys
import json
import datetime
from typing import Tuple, List
import requests
import settings as stgs
import random
from urllib.request import urlopen
from givenergy import GivEnergyObj
	
# Copyright 2023, Steve Lewis. All rights reserved. (see palm.py for full comments and license info)
# NOTE: Simplified version of palm_soc by Graham Hobson created 27 May 2023 designed to be run once a day to
# set max battery recharge level based on a simple forecast of solar generation tomorrow. Simplifications are:
# no load calcs, no consideration of month of the year, simplified solcast retrieval, only use estimate_50
# values. Two new values in settings.py: good_generation_day_kwh and bad_generation_day_kwh

PALM_VERSION = "v0.8.6SoC-Simple1.0"
# -*- coding: utf-8 -*-

def get_solcast_forecast():
    forecast = 0.0

    # get full URL to call solcast with API key
    solcast_url = stgs.Solcast.url_se + stgs.Solcast.cmd + "&api_key=" + stgs.Solcast.key
    
    # check if we are running this before 2am, if so get forecast for today's date, but otherwise get tomorrow
    if datetime.datetime.now().hour < 2:
        offset = 0    # use today's date
    else:
        offset = 1    # use tomorrow's date
    # get the target date in YYYY-MM-DD format (to compare with the json period_end data)
    forecast_date = (datetime.date.today() + datetime.timedelta(days=offset)).strftime('%Y-%m-%d')
    
    jsondata = json.loads(urlopen(solcast_url).read())
    for rec in jsondata['forecasts']:
        this_estimate = rec['pv_estimate']/2 # divide estimate by 2 top get kWh because these are 30 minute time periods
        this_period = rec['period_end']
        if this_period[0:10] == forecast_date:
            forecast += this_estimate
    return forecast
# End of get_solcast_forecast() function definition

def mylogger(logtext):
    
    with open("palm_soc_simple_log.log", "a") as file1:
        timestringnow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file1.write(timestringnow + " " + logtext)
        print(timestringnow + " " + logtext)
        file1.write("\n")
        file1.close()
# End of mylogger() function definition

# --------------------------------------------------------------------------------------
# Start of main
# --------------------------------------------------------------------------------------
if __name__ == '__main__':

    pv_forecast = 0.0
    target_soc: int = 100
    
    mylogger("palm_soc_simple.py: PV Automated Load Manager " + PALM_VERSION)
    print("Runs the forecast collection and inverter update immediately with simple calculation")
    print("Command line options (only one can be used):")
    print("    -t | --test  | test mode... no Solcast calls, no GE writes")
    print("    -d | --debug | debug mode, extra verbose")

    # Parse any command-line arguments
    TEST_MODE: bool = False
    DEBUG_MODE: bool = False
    
    if len(sys.argv) > 1:
        if str(sys.argv[1]) in ["-t", "--test"]:
            TEST_MODE = True
            DEBUG_MODE = True
            mylogger("Info: Running in test mode... no Solcast calls, no GE writes")
        elif str(sys.argv[1]) in ["-d", "--debug"]:
            DEBUG_MODE = True
            mylogger("Info: Running in debug mode, extra verbose")

    # GivEnergy power object initialisation
    ge: GivEnergyObj = GivEnergyObj()

    # use new settings values to get expected generation on a good day, set this to a default if zero
    if stgs.Solcast.good_generation_day_kwh == 0:     # check if this is not set in settings.py 
        stgs.Solcast.good_generation_day_kwh = 30
        
    mylogger("Good/bad generation days: " + str(stgs.Solcast.good_generation_day_kwh) + "/" + str(stgs.Solcast.bad_generation_day_kwh) + "kWh, min_soc_target: " + str(stgs.GE.min_soc_target) +"%")

    if TEST_MODE: # use a random value for tomorrow's solar generation
        pv_forecast = random.randrange(0, int(stgs.Solcast.good_generation_day_kwh * 1.3)) # represents kWh tomorrow
    else: # get the actual solar forecast in kWh
        pv_forecast = get_solcast_forecast()

    # Calculate max overnight charge based on expectation of solar recharge tomorrow
    # Use a bit of maths to set inverse relationship between target_soc and pv_forecast
    # if pv_forecast is at or above good_generation_day_kwh then the battery level will be set to min_soc_target
    # At the other end of the scale if pv_forecast is low/zero then target_soc will be 100%, sliding scale in between
    if pv_forecast >= stgs.Solcast.good_generation_day_kwh:
        target_soc = stgs.GE.min_soc_target
    elif pv_forecast <= stgs.Solcast.bad_generation_day_kwh:
        target_soc = 100
    else:
        charge_range = 100 - stgs.GE.min_soc_target
        generation_range = stgs.Solcast.good_generation_day_kwh - stgs.Solcast.bad_generation_day_kwh
        charge_reduction_factor = (pv_forecast - stgs.Solcast.bad_generation_day_kwh ) / generation_range
        target_soc = int(stgs.GE.min_soc_target + charge_range - (charge_reduction_factor * charge_range))
                
    mylogger("Tom Fcast Gen: " + str(pv_forecast) + "kWh, SoC Target: " + str(target_soc) +"%")

    if not TEST_MODE:
        # Write final SoC target to GivEnergy register
        ge.set_mode("set_soc", str(target_soc))
        mylogger("calling set_mode(set_soc) to set register 77")

    mylogger("------ end ------\n")
# End of main
