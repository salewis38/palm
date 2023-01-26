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
New file palm_soc.py contains only the code to calculate the optimal SoC for overnight (ac) charging of GivEnergy battery. To use this, you will need to download palm_soc.py and settings.py and add your own Solcast API, plant and login details to settings.py.

Use the command "palm_soc.py -t" for test, drop the "-t" argument for the target value to be written to the GivEnergy server

The conservativeness of the forecast works for me. It can be altered by changing the parameters for the final ge_compute_soc command in palm_soc.py 

For full functionality, use palm.py with the additional server data in settings.py for PVOutput.org, etc.

INSTALLATION INSTRUCTIONS FOR RASPBERRY PI
1. Create local directories:
    $ mkdir /home/pi/palm
    $ mkdir /home/pi/logs

2. Download all files to /home/pi/palm/
    $ cd /home/pi/palm
    $ wget github.com/salewis38/palm/archive/heads/main.zip
    $ unzip main.zip
    $ cp -rp palm-heads-main/* ./
    
3. Edit settings.py with your system details, etc
    $ nano settings.py

4. Run palm.py or palm_soc.py, initially in test mode with the command:
    $ ./palm.py -t

5. To run as a persistent service, execute the following commands:
    $ sudo cp palm.service /lib/systemd/system
    $ sudo systemctl start palm.service
    $ sudo systemctl enable palm.service
 
    This will run palm.py in the background and save date-coded logfiles to /home/pi/logs
    
 6. Enjoy!
