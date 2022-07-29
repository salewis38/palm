# palm
PV Automated Load Manager

This software forms the basis of a simple home automation system, using the following hardware:

GivEnergy AC-coupled inverter with AC115 power monitoring on supply and local PV generation
MiHome remote controlled switches for various loads, controlled via hub and API

It performs the following functions:

* aggregating data from multiple sources and upload to PVOutput.org every for 5 minutes for long-term analysis

* setting overnight battery charge target for off-peak electricity usage, based on forecast generation and previous day's consumption

* a simple sequencer for load management, based on time, temperature, battery charge and CO2/kWh

UPDATE 29/Jul/2022
New file palm_soc.py contains only the code to calculate the optimal SoC for overnight (ac) charging of GivEnergy battery. To use this, you will need to download palm_soc.py and settings.py and add your own solcat API, plant and login details to settings.py.

Use the command "palm_soc.py -t" for test, drop the "-t" argument for the target value to be written to the GivEnergy server

The "conservativeness" of the forecast seems to work for me. can be altered by changing the parameters for the final ge_compute_soc command in palm_soc.py 

For full functionality, use palm.py with the additional server data in settings.py for PVOutput.org, etc.
