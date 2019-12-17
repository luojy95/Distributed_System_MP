README: CRANE STORM SYSTEM, author: jiayil5, zjiang30

This mp4 contains the following folder:

-mp4: contains the main file, sdfs.go, README,  two csv files and three sample test yaml files (just for demo purpose).

-output: includes all the generated output files from this Crane System

all the other folders are the same usage from mp3. For the demo, we close this service, user can reopen it from the sdfs.go file (main func).
    
To start this system, 
Step 1:
Stay at this folder and run command "go run sdfs.go" for all the nodes including masters (one real master and one stand-by master).

Step 2:
Except masters, run command "join" and then the master will automatically build the whole network and run it.

Step 3:
Run command "crane xxx.yml" at any worker node (not include the master node). And the system will automatically start running the task. Every noe will automatically print out "Count: %d" every 50 jobs accomplished.

Command 1:
If you want to check how many jobs have been done so far, type in "jobs" to get the value.

Command 2:
During anytime, simply type in "return 1 xxx" and the result will be export to an output file named "xxx" under "output" folder 

Other commands are the same in mp1. Skipped here...

