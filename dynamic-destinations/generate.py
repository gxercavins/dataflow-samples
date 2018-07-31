from random import randint
from lorem.text import TextLorem

import csv

NUM_DIFFERENT_SCHEMAS = 10 # num output tables
NUM_RECORDS = 100

out = csv.writer(open("input.csv","w"))

for i in range(NUM_RECORDS):
    rnd = randint(1,NUM_DIFFERENT_SCHEMAS)
    lorem = TextLorem(wsep=',', srange = (rnd,rnd))

    out.writerow([str(rnd) + ',' + lorem.sentence()[:-1]])

print 'input.csv file created'
