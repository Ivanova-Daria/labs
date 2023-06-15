import luigi
import pandas as pd
class LoadData(luigi.Task):

	def output(self):
		return luigi.LocalTarget("data.csv")

	def run(self):
		df = pd.read_csv('/home/daria/data/merged.csv')
		df.to_csv(self.output().path, index=False)
class DataGood(luigi.Task):
	def output(self):
		return luigi.LocalTarget("data_clean.csv")

	def requires(self):
		return LoadData()

	def run(self):
		df = pd.read_csv(self.input().path)
		df['Order Date'] = pd.to_datetime(df['Order Date'], format='%d.%m.%Y')
		df['Ship Date'] = pd.to_datetime(df['Ship Date'], format='%d.%m.%Y')
		df.to_csv(self.output().path, index=False)


class DataPreprocessing(luigi.Task):
	def output(self):
		return luigi.LocalTarget("data_clean.csv")
	def requires(self):
		return DataGood()
	def run(self):
		df = pd.read_csv(self.input().path)
		cols_to_keep = ["Order Date", "Sales", "Profit", "Quantity", "Discount"]
		df = df[cols_to_keep]
		df = df.fillna({"Discount": 0})
		df = df.dropna()
		df["Order Date"] = pd.to_datetime(df["Order Date"])
		df["Month"] = df["Order Date"].dt.month
		df.to_csv(self.output().path, index=False)


class AggregateData(luigi.Task):
	def output(self):
		return luigi.LocalTarget("sales_report.csv")
	def requires(self):
		return DataPreprocessing()
	def run(self):
		df = pd.read_csv(self.input().path)
		df_agg = df.groupby("Month").agg({
			"Sales": "sum",
			"Profit": "sum",
			"Quantity": "sum",
			"Discount": "mean"
			}).reset_index()
		df_agg.to_csv(self.output().path, index=False)


class PrintReport(luigi.Task):
	def requires(self):
		return AggregateData()

	def run(self):
		df = pd.read_csv(self.input().path)
		print("Sales Report:\n")
		print(df.head()) 
		if __name__ == "__main__":
			luigi.build([PrintReport()], local_scheduler=True) 

class VisualizeData(luigi.Task):
	def output(self):
		return luigi.LocalTarget("sales_report.png")
	def requires(self):
		return AggregateData()

	def run(self):
		import matplotlib.pyplot as plt
		df = pd.read_csv(self.input().path)
		fig, ax = plt.subplots()
		ax.plot(df["Month"], df["Sales"], label="Sales")
		ax.plot(df["Month"], df["Profit"], label="Profit")
		ax.set_xlabel("Month")
		ax.set_ylabel("Amount")
		ax.legend()
		fig.savefig(self.output().path)
class SortBYCategory(luigi.Task):
	def output(self):
		return luigi.LocalTarget("sorted_by_category.csv")
	def requires(self):
		return LoadData()
	def run(self):
		
		df = pd.read_csv(self.input().path)
		df = df.groupby("Sub-Category"
)["Profit"].sum().reset_index()
		df.to_csv(self.output().path, index=False)
class VisualizeProfits(luigi.Task):
	def output(self):
		return luigi.LocalTarget("profit_report.png")
	def requires(self):
		return LoadData()

	def run(self):
		import plotly.express as px
		import matplotlib.pyplot as plt
		df = pd.read_csv(self.input().path)
		df1 = df.groupby("Sub-Category"
)["Profit"].sum().reset_index()
		fig, ax = plt.subplots()
		fig = px.bar(df1, x = "Sub-Category", y="Profit",
		title = "profit report")
		fig.write_image(self.output().path)
			
		
