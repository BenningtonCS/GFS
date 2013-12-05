
print "start"


with open('2mb1itr/4mb1itr/1/appendTest1.txt', 'r') as datafile:
	data = datafile.read().splitlines()


data = filter(None, data)
data = data[1:]

total = 0

for item in data:
	total += float(item)

avg = total / len(data)

print avg