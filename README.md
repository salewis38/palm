# palm
PV Automated Load Manager

This software forms the basis of a simple home automation system, using the following hardware:

GivEnergy AC-coupled inverter with AC115 power monitoring on supply and local PV generation
MiHome remote controlled switches for various loads, controlled via hub and API

It performs the following functions:

* aggregating data from multiple sources and upload to PVOutput.org every for 5 minutes for long-term analysis

* setting overnight battery charge target for off-peak electricity usage, based on forecast generation and previous day's consumption

* a simple sequencer for load management, based on time, temperature, battery charge and CO2/kWh
