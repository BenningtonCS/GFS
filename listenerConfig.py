# Configuration log for listener.py

# name of the log to keep CPU and other
# system information
logName = "listenerLog.log"

# the full log is the same as logName, but
# it keeps a full record of everything
fullLog = "listenerFull.log"

# name of log where errors are stored
errorLogName = "listenerErrors.log"

# the number of items per line in logName
numberOfItemsPerLine = 60

# delay time (in seconds) between when the
# code runs again
delayTime = 10

# files that are deemed necessary to be
# present. If these files are missing from
# the directory, this is a huge problem!
files = ["hosts.txt", "config.py"]
