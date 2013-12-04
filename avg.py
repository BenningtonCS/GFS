
print "start"


with open('8mb1itr/3/scrubTest1.txt', 'r') as datafile:
	data = datafile.read().splitlines()


data = filter(None, data)
data = data[1:]

total = 0

for item in data:
	total += float(item)

avg = total / len(data)

print avg