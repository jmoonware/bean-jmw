# Simple script to plot net worth (Assets) progression generated from 
# saving the GrandTotal line from bcr.py with the '-pt' flag

import pandas as pd 
from datetime import datetime as dt 
import matplotlib.pyplot as plt
import numpy as np 
import yfinance as yf
import argparse
import sys

ap = argparse.ArgumentParser()
ap.add_argument('-f','--file_name',required=True,help='Name of input tsv file; file has four tab-sep columns of start_date, end_date, dontcare, and Value without a header line',default='')
ap.add_argument('-t','--ticker',required=False,help='Plot this ticker on second y axis (SPY is commonly used here)',default='')
ap.add_argument('-i','--inflation',required=False,help='Plot inflation adjusted values using this (yearly) inflation rate',default=0)
ap.add_argument('-og','--output_graph',required=False,help='Save plot to this file, otherwise plot to screen',default='')

clargs = ap.parse_args(sys.argv[1:])

df=pd.read_csv(clargs.file_name,sep='\t',header=None,names=['start_date','end_date','x','Value'])

start_dates = [dt.fromisoformat(d) for d in df['start_date']]
end_dates = [dt.fromisoformat(d) for d in df['end_date']]

fig, ax = plt.subplots()

p0 = ax.plot(end_dates,df['Value'],label='NW USD')

# correct past for inflation
if float(clargs.inflation) > 0:
	r_inflation=float(clargs.inflation) # that's what they say...
	year_fraction = np.array([(dt.now()-d).total_seconds()/(24*365*3600) for d in end_dates])
	p1 = ax.plot(end_dates,df['Value']*np.exp(year_fraction*r_inflation),label='NW USD({0:d})'.format(dt.now().year))

# compare to a stock or index ticker
if len(clargs.ticker) > 0:
	ax2 = ax.twinx()
	tk = yf.ticker.Ticker(clargs.ticker)
	ydf = tk.history(start=min(df['start_date']),end=max(df['end_date']),interval='5d')
	# sometimes start_dates=end_dates...
	ticker_data=[]
	t_s = ydf.index.to_series() # time series
	tz = t_s[0].tz # time zone, assume all same
	for ds,de in zip(start_dates,end_dates):
		t_d = ydf['Close'][t_s.between(ds.isoformat(),de.isoformat())]
		if len(t_d) > 1:
			ticker_data.append(np.mean(t_d))
		else: # find closest time
			idx = np.argmin(np.abs(t_s-dt(de.year,de.month,de.day,tzinfo=tz)))
			ticker_data.append(ydf['Close'][idx])
	p2 = ax2.plot(end_dates,ticker_data,label=clargs.ticker,color='red',linestyle='--')
#	ax2.set_ylim([0,max(ticker_data)*1.04])
	ax2.legend(loc='center right')
	
ax.legend(loc='upper left')

if len(clargs.output_graph) > 0:
	plt.savefig(clargs.output_graph)
else:
	plt.show()
