import luigi
import psycopg2
import csv

class ExportOrders(luigi.Task):
	def requires(self):
		return None

	def output(self):
		return luigi.LocalTarget('/home/daria/data/orders.csv')

	def run(self):
	# Подключаемся к базе данных
		conn = psycopg2.connect(
		dbname='data',
		user='root',
		password='1534&351Da',
		host='daria-VirtualBox',
		port='33060'
		)

		# Получаем данные из таблицы "orders"
		cur = conn.cursor()
		cur.execute('SELECT "Order ID", "Order Date", "Ship Date", "Ship Mode" FROM orders')
		rows = cur.fetchall()

		# Записываем данные в CSV файл
		with self.output().open('w') as f:
			writer = csv.writer(f)
			writer.writerow(['Order ID', 'Order Date', 'Ship Date', 'Ship Mode'])
		for row in rows:
			writer.writerow(row)
