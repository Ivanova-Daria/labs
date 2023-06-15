import luigi
import pandas as pd
fl1 = '/home/daria/data/orders.csv'
fl2 = '/home/daria/data/customer.csv'
out = '/home/daria/data/merged.csv'
class Merge(luigi.Task):
	def requires(self):
		return []
	def output(self):
		return luigi.LocalTarget(out)
	def run(self):
		f1 = pd.read_csv(fl1,encoding='windows-1252', sep=';')
		f2 = pd.read_csv(fl2,encoding='windows-1252',sep=';')
		merged=pd.merge(f1,f2, on='Order ID',how='left')
		merged.to_csv(out, index=False)
