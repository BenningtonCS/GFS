
# USED TO TEST THE MASTER HANDLE INPUT FUNCTION

# Although it is a simple function, it is one of the only functions 
# that can be tested without setting up a TCP connection. It is 
# responsible for making sure that data gets parsed properly so the 
# rest of the object can use it, so it serves an important role.




import master

# create a dummy instance of the handleCommand object
m = master.handleCommand("127.0.0.1", 9978, "sock", "pseudodata", "lock")


print "--------------------------------------------------------------"
print "CHECKING MASTER HANDLE INPUT FUNCTION..."
print "--------------------------------------------------------------"

print "Input == the|quick|brown|fox"
data = m.handleInput("the|quick|brown|fox")
print "Output == ", data
print "\n"
print "Input == ||"
data = m.handleInput("||")
print "Output == ", data
print "\n"
print "Input == what if there is only an end pipe|"
data = m.handleInput("what if there is only an end pipe|")
print "Output == ", data
print "\n"
print "Input == "
data = m.handleInput("")
print "Output == ", data
print "\n"

