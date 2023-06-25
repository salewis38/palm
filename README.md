# palm
PV Automated Load Manager

This software forms the basis of a simple home automation system, using the following hardware:

GivEnergy AC-coupled inverter with AC115 power monitoring on supply and local PV generation
MiHome remote-controlled switches for various loads, controlled via hub and API

It performs the following functions:

* aggregating data from multiple sources and upload to PVOutput.org every for 5 minutes for long-term analysis

* setting overnight battery charge target for off-peak electricity usage, based on forecast generation and previous day's consumption

* a simple sequencer for load management, based on time, temperature, battery charge and CO2/kWh

UPDATE 30/May/2023
File palm_soc.py is now retired as the functionality has been merged back into palm.py. The settings.py file has had minor updates to accommodate this change.

palm.py also includes ONCE_MODE "palm.py -o" to run through the overnight calculations and then exit.

Use the command "palm.py -t" for test, drop the "-t" argument for the target value to be written to the GivEnergy server

The conservativeness of the forecast works for me and is set at 35%. It can be altered by changing the "Solcast.weight" parameter in settings.py 

INSTALLATION INSTRUCTIONS FOR LINUX-BASED SYSTEMS, INCLUDING HOW TO RUN AS A SERVICE ON RASPBERRY PI
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

4. Run palm.py, initially in test mode with the command:

    $ ./palm.py -t

5. To run as a persistent service, execute the following commands:

    $ sudo cp palm.service /lib/systemd/system
 
    $ sudo systemctl start palm.service
    
    $ sudo systemctl enable palm.service
 
    This will run palm.py in the background and save date-coded logfiles to /home/pi/logs
    
 6. Enjoy!

INSTALLATION INSTRUCTIONS FOR WINDOWS-BASED SYSTEMS
1.    Install Python 3 from python.org. Palm.py works with versions of Python >= 3.9
2.    Create a working directory
3.    Download palm.py and settings.py to the working directory
4.    Edit settings.py with your system details
5.    Using a command window, navigate to the working directory and run palm.py, initially with the -t option to run in test mode
6.    If there are any missing library dependencies from the Python install, these are added using the command "pip3 install [modulename]"

