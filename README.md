# palm
PV Automated Load Manager

This software forms the basis of a simple home automation system, using the following hardware:

GivEnergy AC-coupled inverter with power monitoring on supply and local PV generation
MiHome remote-controlled switches for various loads, controlled via hub and API

It performs the following functions:

* aggregating data from multiple sources and upload to PVOutput.org every for 5 minutes for long-term analysis

* setting overnight battery charge target for off-peak electricity usage, based on forecast generation and previous days' consumption

* a simple sequencer for load management, based on time, temperature, battery charge and CO2/kWh

UPDATE 18/Aug/2023
File palm_soc.py is used by the GivTCP add-on to Home Assistant. The main file for execution outside the HA environment is palm.py. This includes ONCE_MODE "palm.py -o" to run through the overnight calculations and then exit.

To simplify code maintenance:
* the settings.py file has been renamed to palm_settings.py for consistency with GivTCP.
* common functionality for all variants of PALM has been moved to a separate palm_utils.py file.

Use the command "palm.py -t" for test, drop the "-t" argument for the target value to be written to the GivEnergy server

PALM now outputs a set of plot data that summarises the SoC calculation. To generate a chart, paste the CSV data into Excel.

The conservativeness of the forecast works for me and is set at 35%. It can be altered by changing the "Solcast.weight" parameter in settings.py 

PALM now includes an Overmorrow function to increase self-consumption in changeable weather.

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
Method 1 (simplest):
1.    Create a working directory
2.    Download palm.exe and settings.py to the working directory
3.    Edit settings.py with your system details
4.    Using a command window, navigate to the working directory and run palm.exe, initially with the -t option to run in test mode

Method 2 (harder, but more versatile):
1.    Install Python 3 from python.org. Palm.py works with versions of Python >= 3.9
2.    Create a working directory
3.    Download palm.py and settings.py to the working directory
4.    Edit settings.py with your system details
5.    Using a command window, navigate to the working directory and run palm.py, initially with the -t option to run in test mode
6.    If there are any missing library dependencies from the Python install, these are added using the command "pip3 install [modulename]"

INSTALLATION INSTRUCTIONS FOR SYNOLOGY NAS (credits to BoffinBoy)

Palm will not run natively on Synology as the installed version of python does not include the relevant packages. Running as a Docker container can help resolve this. A Dockerfile is provided to simplify the process.

Please note, to make any changes to the image, i.e. updating the settings file, it is most reliable to create a new version of the image with a different name in step 5, otherwise you may find the running Containers don’t use the new version.

1.    Extract palm to a directory on your Synology NAS, and edit the settings file as desired
2.    Go to Control Panel > Terminal & SNMP > Enable SSH service
3.    SSH into your Synology NAS
4.    Navigate to the directory you have extracted palm to using: cd /volume1/your/palm-directory
5.    Create a Docker image using: sudo docker build -t desired-image-name .
6.    Once complete, depending on Synology version go to Docker or Container Manager and select the “Container” menu; the instructions below assume you have Container Manager, the menus are slightly different in Docker, but should still generally match the below
7.    Click “Create” and I’m the Image field select the image you just created
8.    Give the Container a name under “Container name”
9.    Tick “Enable auto restart” if you want palm to load automatically when Synology reboots, or if it experiences an error - this is recommended for typical use
10.    Under “Environment” add a new environment variable called TZ with value Europe/London (or your relevant timezone); this is important to ensure the Container is aware of daylight savings
11.    Under “Execution Command” enter python3 palm.py (note: if you want to pass through any additional arguments you should enter them here as well, e.g. python3 palm.py -o if you want to have a version that you run once, when needed)
12.    Complete the wizard, and the container should start running; you can look at the “Log” tab of the running container each day to check all has worked OK
13.    Return to Control Panel > Terminal & SNMP > Disable SSH service; important to do this for security purposes

