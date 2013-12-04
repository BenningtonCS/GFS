
print "start"


with open('aws64mb1itr/scrubTest1.txt', 'r') as datafile:
	data = datafile.read().splitlines()


data = filter(None, data)
data = data[1:]

total = 0

for item in data:
	total += float(item)

avg = total / len(data)

print avg