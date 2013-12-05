
print "start"


with open('64mb1itr/2/scrubTest1.txt', 'r') as datafile:
	data = datafile.read().splitlines()


data = filter(None, data)
data = data[1:]

total = 0

for item in data:
	total += float(item)

avg = total / len(data)

print avg