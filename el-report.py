#!/usr/bin/python3
# -*- coding: utf-8 -*-

import psycopg2
from datetime import datetime


#dt_from='2024-08-01'
#dt_to='2024-09-01'
dt_from='2025-01-01'
dt_to='2025-02-01'


dt_from_val = datetime.strptime(dt_from, '%Y-%m-%d')
dt_to_val = datetime.strptime(dt_to, '%Y-%m-%d')
num_days = (dt_to_val - dt_from_val).days
#print(num_days)


arr=[\
	['Тулэнерго', 1, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], \
	['Владимирэнерго', 2, 22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], \
	['Ивановоэнерго', 3, 33, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], \
	['Калугаэнерго', 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], \
	['Кировэнерго', 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], \
	['Мариэнерго', 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], \
	['Нижновэнерго', 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], \
	['Рязаньэнерго', 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], \
	['Удмуртэнерго', 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 99], \
	]


"""
print(len(arr))
for row in arr :
  print(row)

arr.append(['Всего', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

print('element=',arr[8][1])

print(len(arr))
for row in arr :
  print(row)

xx = len(arr[0])
yy = len(arr)

print('xx=',xx)
print('yy=',yy)

for x in range(1,xx) :
  for y in range(yy-1) :
    arr[yy-1][x] += arr[y][x]

print(len(arr))
for row in arr :
  print(row)
"""


conn = psycopg2.connect(
dbname="podis15",
user="podis",
password="podis_prod",
host="10.52.48.231",
port="5432"
)

cur = conn.cursor()

# Столбец B(1)
with open("req-b.sql", 'r', encoding='utf-8') as file:
  sql_req = file.read().format(dt_to)
cur.execute(sql_req)
result = []
result = cur.fetchall()
print(result)
for i in range(len(result)) :
  for ii in range(len(arr)) :
    if result[i][0] == arr[i][0] :
      arr[i][1] = result[i][1]

# Столбец C(2)
with open("req-c.sql", 'r', encoding='utf-8') as file:
  sql_req = file.read().format(dt_to)
cur.execute(sql_req)
result = []
result = cur.fetchall()
print(result)
for i in range(len(result)) :
  for ii in range(len(arr)) :
    if result[i][0] == arr[i][0] :
      arr[i][2] = result[i][1]

# Столбец D(3)
with open("req-d.sql", 'r', encoding='utf-8') as file:
  sql_req = file.read().format(dt_to)
cur.execute(sql_req)
result = []
result = cur.fetchall()
print(result)
for i in range(len(result)) :
  for ii in range(len(arr)) :
    if result[i][0] == arr[i][0] :
      arr[i][3] = result[i][1]

# Столбец E(4)
with open("req-e.sql", 'r', encoding='utf-8') as file:
  sql_req = file.read().format(dt_to)
cur.execute(sql_req)
result = []
result = cur.fetchall()
print(result)
for i in range(len(result)) :
  for ii in range(len(arr)) :
    if result[i][0] == arr[i][0] :
      arr[i][4] = result[i][1]

# Столбец F(5)
for i in range(len(arr)) :
  arr[i][5] = arr[i][2]*num_days

# Столбец G(6)
with open("req-g.sql", 'r', encoding='utf-8') as file:
  sql_req = file.read().format(dt_from,dt_to)
print(sql_req)
cur.execute(sql_req)
result = []
result = cur.fetchall()
print(result)
for i in range(len(result)) :
  for ii in range(len(arr)) :
    if result[i][0] == arr[i][0] :
      arr[i][6] = result[i][1]

# Столбец H(7)
for i in range(len(arr)) :
  arr[i][7] = round(arr[i][6]/arr[i][5]*100 ,2)

# Столбец I(8)
with open("req-i.sql", 'r', encoding='utf-8') as file:
  sql_req = file.read().format(dt_from, dt_to)
cur.execute(sql_req)
result = []
result = cur.fetchall()
print(result)
for i in range(len(result)) :
  for ii in range(len(arr)) :
    if result[i][0] == arr[i][0] :
      arr[i][8] = result[i][1]

# Столбец J(9)
for i in range(len(arr)) :
  arr[i][9] = round(arr[i][8]/arr[i][5]*100 ,2)

# Столбец K(10)
with open("req-k.sql", 'r', encoding='utf-8') as file:
  sql_req = file.read().format(dt_from, dt_to)
cur.execute(sql_req)
result = []
result = cur.fetchall()
print(result)
for i in range(len(result)) :
  for ii in range(len(arr)) :
    if result[i][0] == arr[i][0] :
      arr[i][10] = result[i][1]

# Столбец L(11)
for i in range(len(arr)) :
  arr[i][11] = round(arr[i][10]/arr[i][5]*100 ,2)

# Столбец M(12)
with open("req-m.sql", 'r', encoding='utf-8') as file:
  sql_req = file.read().format(dt_from, dt_to)
cur.execute(sql_req)
result = []
result = cur.fetchall()
print(result)
for i in range(len(result)) :
  for ii in range(len(arr)) :
    if result[i][0] == arr[i][0] :
      arr[i][12] = result[i][1]

# Столбец N(13)
for i in range(len(arr)) :
  arr[i][13] = round(arr[i][12]/arr[i][5]*100 ,2)

cur.close()
conn.close()


arr.append(['Всего', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])


xx = len(arr[0])
yy = len(arr)
"""
for x in range(1,xx) :
  for y in range(yy-1) :
    arr[yy-1][x] += arr[y][x]
"""
for x in 1,2,3,4,5,6,8,10,12 :
  for y in range(yy - 1):
    arr[yy - 1][x] += arr[y][x]

arr[-1][7] = round(arr[-1][6]/arr[-1][5]*100 ,2)
arr[-1][9] = round(arr[-1][8]/arr[-1][5]*100 ,2)
arr[-1][11] = round(arr[-1][10]/arr[-1][5]*100 ,2)
arr[-1][13] = round(arr[-1][12]/arr[-1][5]*100 ,2)

for row in arr :
  print(row)


