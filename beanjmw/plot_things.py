
from beancount.loader import load_file
from beancount.query import query
import pandas as pd 
import matplotlib.pyplot as plt 
import numpy as np 
from datetime import datetime as dt
from scipy.optimize import curve_fit
from . import dissect as ds
import yfinance as yf
import os
import argparse
import sys
from datetime import datetime as dt
from datetime import timedelta

# linear fit func
def fitfun(x,m,b,y0):
	return(m*(x-b)+y0)

ap = argparse.ArgumentParser()
ap.add_argument('-a','--account',required=False,help='Account regex',default='Groceries')
ap.add_argument('-f','--ledger_file',required=False,help='Ledger file',default='')
ap.add_argument('-pf','--price_file',required=False,help='Prices file',default='prices.txt')
ap.add_argument('-d','--dump',required=False,help='Dump search results (for debugging)',default=False,action='store_true')
ap.add_argument('-e','--error_bars',required=False,help='add 1 std error bars',default=False,action='store_true')
ap.add_argument('-b','--balance',required=False,help='Plot balance by point',default=False,action='store_true')
ap.add_argument('-cb','--cost_basis',required=False,help='Plot cost basis by point (along with balance)',default=False,action='store_true')
ap.add_argument('-p','--points',required=False,help='Plot change by point',default=False,action='store_true')
ap.add_argument('-rc','--report_currency',required=False,help='Name of commidity, or USD (default)',default='USD')
ap.add_argument('-og','--output_graph',required=False,help='Name of plot image file to save - otherwise plot to screen',default='')
ap.add_argument('-od','--output_data',required=False,help='Name of tsv file of plot data to save',default='')
ap.add_argument('-sd','--start_date',required=False,help='Start date (iso format, default one year ago)',default=dt.date(dt.now()-timedelta(days=365)).isoformat())
ap.add_argument('-ed','--end_date',required=False,help='End date (iso format, default today)',default=dt.date(dt.now()).isoformat())

clargs = ap.parse_args(sys.argv[1:])

if os.path.isfile(clargs.ledger_file):
	entries, errors, config = load_file(clargs.ledger_file)
else:
	ap.exit(message="Can't find '{0}' - did you specify a ledger file?\n".format(clargs.ledger_file))


data_table={}

qs = 'select account, date, change, balance from open on {0} close on {1} where account~"{2}" order by date'.format(clargs.start_date,clargs.end_date,clargs.account)

qr=query.run_query(entries,config,qs,()) 

accounts = np.unique([r.account for r in qr[1]])
print(accounts)

xdata=np.array([r.date for r in qr[1] if r.change])
if len(xdata)==0:
	sys.stderr.write("No data for {0}\n".format(clargs.account))
	sys.exit()

ylabels = np.unique([r.change[0][1] for r in qr[1] if r.change])
if len(ylabels) > 1:
	sys.stderr.write("Multiple currencies {0}".format(ylabels))
	sys.exit()

ydata = np.array([r.change[0][0] for r in qr[1] if r.change])
quotes=np.full(len(ydata),1)
price_table={}

if ylabels[0]!=clargs.report_currency:
	price_entries=[]
	if os.path.isfile(clargs.price_file):
		price_entries, errors, config = load_file(clargs.price_file)
	price_table = ds.create_price_table(price_entries)
	price_table = ds.create_price_table(entries,price_table=price_table)
	pt_size = ds.size_price_table(price_table)

	tk=yf.ticker.Ticker(ylabels[0])
	quotes=[ds.quote(ylabels[0],tk=tk,prices=price_table,quote_date=r.date) for r in qr[1] if r.change]
	# convert to quote currency
	# NOTE: Quotes are in USD, other national currencies not supported
	quotes = np.array([q.amount[0] for q in quotes]) 
	
if clargs.balance:
	ydata=np.cumsum(ydata)*quotes
	if clargs.cost_basis:
		inv = [r.balance for r in qr[1] if r.change] 
		cb=[]
		for invi in inv:
			wave=0 # weighted ave
			tot=0
			for amt, cost in invi:
				if cost and amt:
					wave+=amt[0]*cost[0]
					tot+=amt[0]
			if tot > 0:
				cb.append(wave/tot)
			else:
				cb.append(0)

min_year=np.min(xdata).year
min_month=np.min(xdata).month
max_year=np.max(xdata).year
max_month=np.max(xdata).month

month_bins = np.array([dt.date(dt(y,m,1)) for y in range(min_year,max_year+1) for m in range(1,13)])

year_bins = np.array([dt.date(dt(y,1,1)) for y in range(min_year,max_year+1)])

monthly_data=np.array([sum(ydata[(xdata>=sd)&(xdata<ed)]) for sd,ed in zip(month_bins[:-1],month_bins[1:])])

monthly_years=[sd.year for sd in month_bins[:-1]]
all_years=np.unique(monthly_years)

fig, ax = plt.subplots()

# plot all points
if clargs.balance or clargs.points:
	data_table['date']=xdata
	data_table[ylabels[0]]=ydata
	ax.plot(xdata,ydata,label=clargs.account,marker='.')
	if clargs.cost_basis and clargs.balance:
		ax2 = ax.twinx()
		if len(quotes)==len(xdata):
			data_table['quotes']=quotes
			ax2.plot(xdata,quotes,label="{0}, Quote".format(clargs.account),color='orange',marker='.')
		ax2.plot(xdata,cb,label="{0}, CB".format(clargs.account),color='green',marker='.')
		data_table['cost_basis']=cb
		ax2.legend(loc='lower right')
		ax2.set_ylabel('USD')

else: # plot by monthly average
	yearly_data=[np.mean(monthly_data[monthly_years==yr]) for yr in all_years]
	if clargs.error_bars:
		yearly_data_std=[np.std(monthly_data[monthly_years==yr]) for yr in all_years]
	else:
		yearly_data_std=[0 for yr in all_years]
	
	data_table['date']=all_years
	data_table['yearly_data']=yearly_data
	data_table['yearly_data_std']=yearly_data_std
	ax.errorbar(all_years,yearly_data,yerr=yearly_data_std, capsize=5,marker='o',label=clargs.account)

	if len(data_table['date']) >= 3:	
		res = curve_fit(fitfun,all_years,yearly_data, p0=[0.03,2000,1000])
		data_table['fit']=fitfun(all_years,res[0][0],res[0][1],res[0][2])
		ave_val=np.mean(data_table['fit'])
		perc_change = 100*res[0][0]/ave_val
		data_table['fit']=fitfun(all_years,res[0][0],res[0][1],res[0][2])
		ax.plot(all_years,data_table['fit'],label='linear {0:.2f}% pa'.format(perc_change))

plt.title(clargs.account)
plt.xlabel("Date")
ax.set_ylabel(clargs.report_currency)
ax.legend(loc='upper left')
if len(clargs.output_graph) > 0:
	plt.savefig(clargs.output_graph)
else:
	plt.show()

if len(clargs.output_data) > 0:
	with open(clargs.output_data,'w') as f:
		f.write('# ' + ' '.join(sys.argv)+'\n')
		f.write('# ' + ', '.join(accounts)+'\n')
		f.write('\t'.join(data_table.keys())+'\n')
		for i,d in enumerate(data_table['date']):
			line='\t'.join([str(d)]+["{0:.2f}".format(data_table[k][i]) for k in data_table if k!='date'])+'\n'
			f.write(line)

# if price table has changed then save a new version
if ylabels[0]!=clargs.report_currency and pt_size!=ds.size_price_table(price_table):
	ds.save_price_table(clargs.price_file,price_table)

if clargs.dump:
	for r in qr[1]:
		print(r.account, r.date, r.change)
