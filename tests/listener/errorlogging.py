import listener, time

# test to see how well logging information to the listener works
# by importing it to another program
for x in range(0, 10):
	listener.logError(x)
	print "Logged " + str(x)
	time.sleep(1)
