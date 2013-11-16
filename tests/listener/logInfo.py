import os, re

logName = 'test.log'
totItems = 5

def logInfo(lineNum, info):
        # logs info from each function below this one into a listener log

        # put the information from the listener log into a var called data
        with open(logName, 'r') as r:
                data = r.readlines()
        print "Read " + str(data) + " from the log."

        # split the data from the line spefied  at the pipe and newline character 
        try: l = re.split('[|\n]', data[int(lineNum)])
        # if there is no data at that line, it will return an index error. If this
        # happens, it will make a list with one character in it that will then
        # be deleted
        except IndexError: l = [0]
        # the last index in the list is deleted as it is an empty string
        del l[-1]

        # if there are more items in the list than there should be as specified
        # in the config file, delete the first item
        while len(l) >= totItems: del l[0]

        # add new data to the end of the list
        l.append(str(info))
        print "Appended " + str(info) + " to the list."

        # turn the data into a string, each part seperated by a pipe
        newData = '|'.join(l)

        try: # try to add the new data to the list of data
                # in the case that the file is empty
                if data == []: data = newData + '\n'
                # in the case that there already is information at that line
                else: data[int(lineNum)] = newData + '\n'
        # in the case that the line that should have new data added to it doesn't 
        # exist yet
        except IndexError: data.append(newData + '\n')

        # add the data to the listener log
        with open(logName, 'w') as w:
                w.writelines(data)

count = [0]
def test(lineNum):
        # test function to make sure that the logging worked
        # this works by just increasing a counter and adding that to the log
        count[0] += 1
        logInfo(lineNum, count[0])

# main loop
while 1:
        test(0)
