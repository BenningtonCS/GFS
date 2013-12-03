Cheese Whiz Design
===================

###Terms:

**Cheese Whiz** - Working name for the "runner" program that makes sure the processes that should be running are running.

**Whizzifest** - The manifest which contains the names and locations (formatted as processName|destination) of the processes that should be active in the current version.

###Requirements:

1. Cheese Whiz should be able to periodically check what processes are running, and which are not running, in relation to what should be running.

2. The list of programs that should be running will be created and maintained manually

3. It should be able to start the programs that should be running and are not, and terminate the programs that are running and should not be.

4. It should do all of this automatically and periodically.


###Components:

* **Chron Job** - allows for automacy and periodicity
	
	the chron job activates the process that reads whizzifest
	
* **Read whizzifest** - reads and sends the manifest of programs that should be running

	read whizzifest is activated by the chron job and passes a list of names and destinations to the process that checks running processes
	
* **Check running processes** - checks which processes are running, returns what is not running that should be

	check running processes receives a list of names and destinations from the read whizzifest process and passes a string of processes that are not running but should be to the run inactive process
	
* **Run inactive** - runs inactive processes that should be active and terminates active processes that should be inactive

	the run inactive process gets a string of processes that need to be executed and executes them.


###Additional Notes:

* The completed program source should live in the master server and get pulled to each of the RPIs by quesofiesta 

* The third step (the process that checks for running processes) should recursively call itself until there are no remaining inactive processes that should be active





