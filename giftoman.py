import fdb
import datetime
import os

# print("Введите код подразделения: ")
# shop_num = input()
# print("Введите путь к БД (пример: 127.0.0.1:D:/base/main.gdb): ")
# path = input()
# print("Необходимо установить диапазон дат за который необходимо выгрузить чеки")
# print("Введите дату 1 с какой даты (формат даты 01.01.2020):")
# date1 = input()
# print("Введите дату 2 по какую дату (формат даты 01.01.2020):")
# date2 = input()

f = open('upload.csv', 'w', encoding="utf-8-sig")

shop_num = 555

path = 'IP:D:/base/fm_sync/MAIN.GDB'
con = fdb.connect(dsn=path, user='user', password='password', charset='WIN1251')
cur = con.cursor()

date1 = '17.03.2020'
date2 = '17.03.2020'

sql = '''select rmk.name as cass, document.chequenumber as checknum,
        document.opendate as data, document.opentime as tim,
        sprt.mark as barcode, sprt.name as name,
        tranzt.quantity,
        tranzt.summwd, tranzt.summ as WD,
        "USER".name as use, document.dockindid,
		tranzt.tranztype
from tranzt
left outer join document on (tranzt.documentid = document.id)
left outer join rmk on (document.openrmkid = rmk.id)
left outer join sprt on (tranzt.warecode = sprt.code)
left outer join "USER" on (document.owneruserid = "USER".id)
where tranzt.tranzdate >= '{0}' and tranzt.tranzdate <= '{1}' and 
	((tranzt.tranztype = 11) or (tranzt.tranztype = 12)) and
        tranzt.price > 1 and
        sprt.mark is not null and
        document.state = '1'
order by checknum asc
'''.format(date1, date2)

os.system('cls')
print("Загружаю данные из базы. Ждите...")

operations = {}
checks = []
csv = {}
cur.execute(sql)

i = 0
for c in cur.fetchall():
	i += 1
	operations[i] = [c[0], c[1], c[2], c[3], c[4], c[5], 
		round(c[6], 2), round(c[7], 2), round(c[8], 2), c[9], c[10], c[11]]
	if not c[1] in checks:
		checks.append(c[1])

con.close()

goods = {}
check = {}

i = 0
for numbers in checks:
	for key, item in operations.items():
		num = item[1]
		code = item[4]
		if num == numbers:
			if not num in check:
				check[num] = [item[0], item[10], []]						
			
			if not code in goods:
				goods[code] = [item[2], item[3], item[5],
					item[6], item[7], item[8],
					item[9], item[11]]
				check[num][2].append(code)
			else:
				goods[code][3] = goods[code][3] + item[6]
				goods[code][4] = goods[code][4] + item[7]
				goods[code][5] = goods[code][5] + item[8]

	j = 0
	for key1, item1 in check.items():
		for key2, item2 in goods.items():
			i += 1; j += 1
			if item2[3] < 0 and item2[4] < 0 and item2[5] < 0:
				item2[3] = abs(item2[3])
				item2[4] = abs(item2[4])
				item2[5] = abs(item2[5])
			
			if item1[1] == 2:
				item1[1] = 0

			if item2[3] != 0 and item2[4] != 0 and item2[5] != 0:
				count = str(round(float(item2[3]), 2)).split(".")
				if len(count[1]) == 1:
					count[1] += '0'
				item2[3] = str(count[0]) + "." + str(count[1])

				disc = round(float(item2[5]) - float(item2[4]), 2)
				disc = str(disc).split(".")
				if len(disc[1]) == 1:
					disc[1] += '0'
				item2[5] = str(disc[0]) + "." + str(disc[1])

				sumwd = str(round(float(item2[4]), 2)).split(".")
				if len(sumwd[1]) == 1:
					sumwd[1] += '0'
				item2[4] = str(sumwd[0]) + "." + str(sumwd[1])

				s = "{0};{1};{2};{3};{4};{5};{6};{7};{8};{9};{10};{11};;{12}\n".format(shop_num,
					item1[0], key1, j, item2[0].strftime("%Y-%m-%d"),
					item2[1].strftime("%H:%M:%S"), key2, item2[2], item2[3],
					item2[4], item2[5],
					item2[6], item1[1])
				f.write(s)
	goods.clear()
	check.clear()
f.write("___EOF___")
f.close()
print("Готово!")