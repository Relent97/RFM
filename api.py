import pandas as pd
import numpy as np
from flask import Flask, request
from flask_restful import Resource, Api
from datetime import timedelta
from flasgger import Swagger
from flasgger.utils import swag_from

app = Flask(__name__)
app.config["SWAGGER"] = {"title": "Kaggle e-commerce Swagger-UI", "uiversion":2}
api = Api(app)


swagger_config = {"headers": [], 
                  "specs": [{"endpoint":"apispec_1",
                  "route": "/apispec_1.json",
                  "rule_filter": lambda rule:True,
                  "model_filter": lambda tag: True,
                  }
                  ],
                  "static_url_path": "/flasgger_static",
                  "swagger_ui":True,
                  "specs_route": "/swagger/",
                  } 

swagger = Swagger(app, config=swagger_config)




D = pd.read_csv('data.csv',encoding= 'unicode_escape')
D['Revenue'] = D['UnitPrice'] * D['Quantity']
D['InvoiceDate'] = pd.to_datetime(D['InvoiceDate'])
D.Description.fillna('None', inplace = True)
D.dropna(inplace=True)

D['Date'] = D['InvoiceDate'].dt.date
D['Week'] = D['InvoiceDate'].dt.week
D['Month'] = D['InvoiceDate'].dt.month
D["Day"] = D["InvoiceDate"].dt.weekday
Dayname = {0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'}
D['Dayn'] = D.Day.map(Dayname)
D['Date'] = pd.to_datetime(D['Date'])

today = D['Date'].max()
yesterday = today - timedelta(days = 1) 
This_week = D["Week"].max()
Last_week = This_week - 1
This_month = D["Month"].max()
Last_month = This_month-1



C = D.groupby('CustomerID').min()['Date'].to_frame().reset_index()
C['Date'] = pd.to_datetime(C['Date'])
C['Month'] = C['Date'].dt.month

Cn = D.groupby("Date")["Country"].value_counts().to_frame()
Cn.columns = ["R"]
Cn.reset_index(inplace = True)
Cn.columns = ["Date","Country","Count"]


Css = D.groupby(['Date','Country']).sum()['Revenue'].to_frame().reset_index()

Dt = []
Cou = []
for x in D.Country.unique():
    a = D[D['Country']==x]['Date'].min()
    Cou.append(x)
    Dt.append(a)
CY = pd.DataFrame(columns = [Dt, Cou])
CY = CY.transpose()
CY.reset_index(inplace=True)
CY.columns = ['Date','Country']
CY['Date'] = pd.to_datetime(CY['Date'])
CY['Month'] = CY['Date'].dt.month
# --Group data by customerID--
# Create snapshot date.
snapshot_date = D['Date'].max() + timedelta(days=1)
print(snapshot_date)
# Grouping by CustomerID





              

RD = []
Cust = []
for x in D.CustomerID.unique():
    a = D[D['CustomerID']==x]['Date'].min()
    Cust.append(x)
    RD.append(a)
Custom = pd.DataFrame(columns = [RD, Cust])
Customers = Custom.transpose()
Customers.reset_index(inplace=True)
Customers.columns = ['Date','CustomerID']
Customers['Date'] = pd.to_datetime(Customers['Date'])
Customers['Dayn'] = Customers.Date.dt.dayofweek
Customers['Day'] = Customers.Dayn.map(Dayname)



class Data1(Resource):
	def get(self):
		D1 = D.to_json()
		return D1



class Monthly(Resource):
	@swag_from("monthly.yml")
	def get(self):
		M1 = D.groupby(['Month']).agg({'InvoiceNo': 'nunique','Revenue': 'sum'})
		M1.rename(columns={'InvoiceNo': 'Transactions','Revenue': 'Revenue'}, inplace=True)
		M1.reset_index(inplace=True) 
		M1['Revenue Per Transaction'] = M1['Revenue']/M1['Transactions']
		M = M1.to_json()
		return M

class Weekly(Resource):
	@swag_from("weekly.yml")
	def get (self):
		W1 = D.groupby(['Week']).agg({'InvoiceNo': 'nunique','Revenue': 'sum'})
		W1.rename(columns={'InvoiceNo': 'Transactions','Revenue': 'Revenue'}, inplace=True)
		W1.reset_index(inplace=True) 
		W1['Revenue Per Transaction'] = W1['Revenue']/W1['Transactions']
		W = W1.to_json()
		return W

class rfm1(Resource):
	@swag_from("rfm.yml")
	def get(self):
		Ra = D.groupby(['CustomerID']).agg({'Date': lambda x: (snapshot_date - x.max()).days,'InvoiceNo': 'count','Revenue': 'sum'}) 
		Ra.rename(columns={'Date': 'Recency','InvoiceNo': 'Frequency','Revenue': 'MonetaryValue'}, inplace=True)
		Ra.reset_index(inplace=True)
		Ra['Count'] = 1
		r_labels = range(4,0,-1); f_labels = range(1, 5); m_labels = range(1, 5)
		r_groups = pd.qcut(Ra['Recency'], q=4, labels=r_labels)
		f_groups = pd.qcut(Ra['Frequency'], q=4, labels=f_labels)
		m_groups = pd.qcut(Ra['MonetaryValue'], q=4, labels=m_labels) 
		Ra = Ra.assign(R_score = r_groups.values, F_score = f_groups.values, M_score = m_groups.values)
		NNear = ["R_score","F_score","M_score"]
		Ra[NNear] = Ra[NNear].applymap(int)
		Ra['Score'] = Ra['R_score'] + Ra['F_score'] + Ra['M_score']
		def rfm_level(Ra):
			if Ra['Score'] >= 9:
				return 'Loyal'
			elif ((Ra['Score'] >= 8) and (Ra['Score'] < 9)):
				return 'Very promising'
			elif ((Ra['Score'] >= 7) and (Ra['Score'] < 8)):
				return 'Promising'
			elif ((Ra['Score'] >= 6) and (Ra['Score'] < 7)):
				return 'To be guided'
			elif ((Ra['Score'] >= 5) and (Ra['Score'] < 6)):
				return 'Getting there'
			elif ((Ra['Score'] >= 4) and (Ra['Score'] < 5)):
				return 'Needs Attention'
			else:
				return 'Bad market'

		Ra['Level'] = Ra.apply(rfm_level, axis=1)
		Ras = Ra.to_json()
		return Ras

class Country(Resource):
	@swag_from("country.yml")
	def get(self):
		CRT = D.groupby(['Date','Country']).agg({'InvoiceNo':'nunique','Revenue': 'sum'})
		CRT.rename(columns={'InvoiceNo': 'Transactions','Revenue': 'Revenue'}, inplace=True)
		CRT.reset_index(inplace=True)
		CR = CRT.to_json()
		return CR           

class CSD2(Resource):
	@swag_from("csd2.yaml")
	def get(self):
		CSD22 = Customers.groupby('Day')['CustomerID'].count().to_frame().reset_index()
		CSD = CSD22.to_json()
		return CSD

class CSDT(Resource):
	@swag_from("csdt.yml")
	def get(self):
		CSDT2 = Customers.groupby('Date')['CustomerID'].count().to_frame().reset_index()
		CST = CSDT2.to_json()
		return CST



@app.route('/')
def hello():
	return("Hello World")





api.add_resource(Data1, '/data')
api.add_resource(Monthly, '/monthly')
api.add_resource(Weekly, '/weekly')
api.add_resource(rfm1, '/rfm')
api.add_resource(Country, '/countries')
api.add_resource(CSD2, '/customeracquisition/day')
api.add_resource(CSDT, '/customeracquisition/date')




if __name__ =='__main__':
	app.run(debug = True)