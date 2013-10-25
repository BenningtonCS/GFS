# Running the Emulator

The chunk server emulator is a stripped down version of the chunk server that only deals with the heartBeat interactions. It allows you to define two things:

1. Response Delay

    Setting this above the heartBeat timeout length should simulate a chunkserver timeout error
  
2. Message

    The default message is what the heartBeat expects to receive, so sending unexpected data simulates a faulty chunkserver send or faulty heartBeat receive
    
    
    
## Things to note:

* In this test emulator, the port is hardcoded in, since it should be the default port used by our GFS implementation. If that port number changes though, the port number in the emulator will need to change also.
* Be sure to update the `hosts.txt` file in the directory with `heartBeat.py` to reflect the machine running the emulator.
