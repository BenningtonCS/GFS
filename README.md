GFS
===

Visit http://backus.bennington.edu



## Initializing the Master

#### Typical Initialization

Running the master is fairly straightforward, assuming the chunkservers are active and the heartBeat is working. Simply type `python master.py` into the command line, or if you wish to enable debugging, `python master.py -v`. 

#### Test Initialization

For a local testing setup, a bit more work has to be done. First, if you are going to use test chunkservers, they will need to be initialized on different machines (this can be done through ssh), and the IPs of those machines will need to be added to `hosts.txt`.

Now, the master can be run `python master.py`, which will also intialize the heartBeat and database. If you wish to interact with the master/database and send commands, you will need an instance of the API client running. It is important to make sure that the `masterip` variable in `config.py` is updated to reflect the IP of the machine that is running your master/database instance, or else the API client will not be able to communicate.

Once testing is complete, be sure to revert any changes to `hosts.txt` or `config.py` before pushing any changes, especially if the updated version will be pulled to backus.