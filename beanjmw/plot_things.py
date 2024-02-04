
from beancount.loader import load_file
from beancount.query import query
import pandas as pd 
import matplotlib.pyplot as plt 
import numpy as np 
from datetime import datetime as dt
from scipy.optimize import curve_fit

import argparse
import sys

# linear fit func
def fitfun(x,m,b,y0):
	return(m*(x-b)+y0)

ap = argparse.ArgumentParser()
ap.add_argument('-a','--account',required=False,help='Account regex',default='Groceries')
ap.add_argument('-f','--ledger_file',required=False,help='Ledger file',default='master.txt')
ap.add_argument('-d','--dump',required=False,help='Dump search results (for debugging)',default=False,action='store_true')
ap.add_argument('-e','--error_bars',required=False,help='add 1 std error bars',default=False,action='store_true')
ap.add_argument('-b','--balance',required=False,help='Plot balance by point',default=False,action='store_true')
ap.add_argument('-p','--points',required=False,help='Plot change by point',default=False,action='store_true')

clargs = ap.parse_args(sys.argv[1:])

entries, errors, config = load_file(clargs.ledger_file)

qs = 'select account, date, change where account~"' + clargs.account + '"'

qr=query.run_query(entries,config,qs,()) 

accounts = np.unique([r.account for r in qr[1]])
print(accounts)

xdata=np.array([r.date for r in qr[1] if r.change])
if len(xdata)==0:
	sys.stderr.write("No data for {0}\n".format(clargs.account))
	sys.exit()

if clargs.balance:
	ydata=np.cumsum(np.array([r.change[0][0] for r in qr[1] if r.change]))
else:
	ydata=np.array([r.change[0][0] for r in qr[1] if r.change])

ylabels = np.unique([r.change[0][1] for r in qr[1] if r.change])
if len(ylabels) > 1:
	sys.stderr.write("Warning: multiple currencies {0}".format(ylabels))

min_year=np.min(xdata).year
min_month=np.min(xdata).month
max_year=np.max(xdata).year
max_month=np.max(xdata).month

month_bins = np.array([dt.date(dt(y,m,1)) for y in range(min_year,max_year+1) for m in range(1,13)])

year_bins = np.array([dt.date(dt(y,1,1)) for y in range(min_year,max_year+1)])

monthly_data=np.array([sum(ydata[(xdata>=sd)&(xdata<ed)]) for sd,ed in zip(month_bins[:-1],month_bins[1:])])

monthly_years=[sd.year for sd in month_bins[:-1]]
all_years=np.unique(monthly_years)

# plot all points
if clargs.balance or clargs.points:
	plt.plot(xdata,ydata,label=clargs.account)

else: # plot by monthly average
	yearly_data=[np.mean(monthly_data[monthly_years==yr]) for yr in all_years]
	if clargs.error_bars:
		yearly_data_std=[np.std(monthly_data[monthly_years==yr]) for yr in all_years]
	else:
		yearly_data_std=[0 for yr in all_years]
	
	plt.errorbar(all_years,yearly_data,yerr=yearly_data_std, capsize=5,marker='o',label=clargs.account)
	
	res = curve_fit(fitfun,all_years,yearly_data, p0=[0.03,2000,1000])
	ave_val=np.mean(fitfun(all_years,res[0][0],res[0][1],res[0][2]))
	perc_change = res[0][0]/ave_val
	plt.plot(all_years,fitfun(all_years,res[0][0],res[0][1],res[0][2]),label='linear {0:.2f}% pa'.format(perc_change*100))

plt.title(clargs.account)
plt.xlabel("Date")
plt.ylabel(ylabels[0])
plt.legend()
plt.show()

if clargs.dump:
	for r in qr[1]:
		print(r.account, r.date, r.change)
