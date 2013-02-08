import time
import csv
import monetdb.sql

t = time.time()
x = monetdb.sql.connect(database="mydb")
c = x.cursor()


csvfile = open('py.tsv','wb')
w = csv.writer(csvfile, delimiter="\t")

for i in range(1000,100001,1000):
  c.arraysize=i
  print(i)
  start = time.time()
  c.execute('select * from partsupp limit ' + str(i))
  results = c.fetchall()
  taken = time.time() - start
  print(taken)
  rs = i/taken
  print(rs)
  w.writerow([i,rs,taken])