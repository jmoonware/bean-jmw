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
ap.add_argument('-f','--file_name',required=True,help='Name of input tsv file; file has at least columns named "start_date" and "Value"',default='')
ap.add_argument('-t','--ticker',required=False,help='Plot this ticker on second y axis (SPY is commonly used here)',default='')
ap.add_argument('-i','--inflation',required=False,help='Plot inflation adjusted values using this inflation rate',default=0)
ap.add_argument('-og','--output_graph',required=False,help='Save plot to this file, otherwise plot to screen',default='')

clargs = ap.parse_args(sys.argv[1:])

df=pd.read_csv(clargs.file_name,sep='\t')

current_year = dt.now().year

yr = np.array([dt.fromisoformat(d).year for d in df['start_date']])

fig, ax = plt.subplots()

p0 = ax.plot(yr,df['Value'],label='NW USD')

if float(clargs.inflation) > 0:
	r_inflation=float(clargs.inflation) # that's what they say...
	p1 = ax.plot(yr,df['Value']*np.exp((2024-yr)*r_inflation),label='NW USD(2024)')

if len(clargs.ticker) > 0:
	ax2 = ax.twinx()
	tk = yf.ticker.Ticker(clargs.ticker)
	ydf = tk.history(start=min(df['start_date']),end=max(df['start_date']),interval='1mo')
	ticker_data = [np.mean(ydf['Close'][ydf.index.to_series().between(dt(y,1,1).isoformat(),dt(y,12,31).isoformat())]) for y in yr]
	p2 = ax2.plot(yr,ticker_data,label=clargs.ticker,color='red',linestyle='--')
	ax2.set_ylim([0,max(ticker_data)*1.04])
	ax2.legend(loc='center right')
	
ax.legend(loc='upper left')

if len(clargs.output_graph) > 0:
	plt.savefig(clargs.output_graph)
else:
	plt.show()
