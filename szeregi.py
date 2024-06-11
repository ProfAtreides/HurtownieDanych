import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from pmdarima import auto_arima
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error, mean_absolute_error
import math

db_config = {
    'user': 'root',
    'password': '12345',
    'host': 'localhost',
    'database': 'data_base'
}

conn = mysql.connector.connect(**db_config)
query = "SELECT * FROM sales"
df = pd.read_sql(query, conn)
conn.close()

df['sales_value'] = df['quantity'] * df['unit_price']
daily_sales = df.groupby('date')['sales_value'].sum().reset_index()
daily_sales = daily_sales.sort_values('date').reset_index(drop=True)
daily_sales['date'] = pd.to_datetime(daily_sales['date'])
daily_sales.set_index('date', inplace=True)

training_data_percentage = 0.7
test_data_percentage = 1 - training_data_percentage

train_data, test_data = daily_sales[0:int(len(daily_sales) * training_data_percentage)], daily_sales[int(len(
    daily_sales) * training_data_percentage):]

train_arima = train_data['sales_value']
test_arima = test_data['sales_value']

model_auto = auto_arima(daily_sales['sales_value'], seasonal=False, trace=True)
order = (0,0,0)


history = [x for x in train_arima]
y = test_arima

# first prediction
predictions = list()
model = ARIMA(history, order=order)
model_fit = model.fit()
yhat = model_fit.forecast()[0]
predictions.append(yhat)
history.append(y[0])

print(model)

for i in range(1, len(y)):
    model = ARIMA(history, order=order)
    model_fit = model.fit()
    yhat = model_fit.forecast()[0]
    predictions.append(yhat)
    obs = y[i]
    history.append(obs)

plt.figure(figsize=(16, 8))
plt.plot(daily_sales.index[-len(daily_sales * training_data_percentage):],
         daily_sales['sales_value'].tail(len(daily_sales * training_data_percentage)), color='green',
         label='Dane treningowe')
plt.plot(test_data.index, y, color='red', label='Dane faktyczne')
plt.plot(test_data.index, predictions, color='blue', label='Predykcja')
plt.title('Zysk sklepowy')
plt.xlabel('Time')
plt.ylabel('Zarobki')
plt.legend()
plt.grid(True)
plt.savefig('arima_model.pdf')
plt.show()
