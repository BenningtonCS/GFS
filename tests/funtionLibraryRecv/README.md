# Running the Emulator

The client and server emulators act as a way to test out the recv() function, in the functionLibrary. They import config and functionLibrary so should be run from a directory containing them.

The server emulator should be initialized before the client emulator

1. Client Emulator

	A simple TCP client which prompts for a port to connect over, a message to send, and whether you would like to send the message with an EOT character or not. See notes for effects of this.
  
2. Server Emulator

    A simple TCP server which prompts for a port to connect over and whether or not the message it sends back to the client should include an EOT character. See notes for effects of this.
    
    
    
## Notes:

* As this simplistic test reveals, sending a message without an EOT character will cause the recv() function to block, preventing any further actions. This is bad and is a good example of why, when implemented, all send() functions MUST include an EOT character. 
* What happens if there is a request received from somewhere outside the system? (not using the EOT protocol) : recv() will hang. Is there a way around this? (timeouts may work but may also force false errors if our legitimate uses for it take too long and time out themselves)
