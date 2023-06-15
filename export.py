import luigi
import mysql.connector
import os
from github import Github

class ExportToMySQL(luigi.Task):
	def output(self):
		return luigi.LocalTarget('/home/daria/data/sales_report.csv')

	def run(self):
		with self.output().open() as f:
		data = f.read()
		conn = mysql.connector.connect(
		host='daria-VirtualBox',
		user='root',
		password='1534&351Da',
		database='data'
		)

		cursor = conn.cursor()
		cursor.execute(f"INSERT INTO {self.db_table} (data) VALUES ('{data}')")
		conn.commit()
		conn.close()

class ImportToGitHub(luigi.Task):
	def output(self):
		return luigi.LocalTarget('/home/daria/data/sales_report.png')

	def run(self):
		g = Github('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')
		repo = g.get_repo('Ivanova-Daria/course_project')
		with self.output().open() as f:
		content = f.read()
		file_name = os.path.basename(self.source_file)
		repo.create_file(self.gh_folder + '/' + file_name, f"import {file_name}", content, branch="main")
