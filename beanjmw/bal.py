# Simple balance checker from csv values
# Plots balances at dates in csv, prints differences, and plots
# useful for visualizing when discrepancies happen

from beancount.query import query
from beancount.loader import load_file
from decimal import Decimal
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import argparse 
import sys
from datetime import datetime as dt

ap=argparse.ArgumentParser()
ap.add_argument('-f','--filename',required=True,help='Beancount ledger file')
ap.add_argument('-bf','--balance_file',required=True,help='date,account(s) csv file, each column is balance by date for an account')
ap.add_argument('-a','--account',required=True,help='Account name (for query and column of balance file')
clargs = ap.parse_args(sys.argv[1:])

balances = pd.read_csv(clargs.balance_file)

if not clargs.account in balances.columns:
	sys.stderr.write("No matching account {0} in {1}\n".format(clargs.account,balances.columns))
	sys.exit()

# load the ledger
entries,errors,config=load_file(clargs.filename)

# for each date in csv balance file, check account differences
dates=[]
ledger_bal=[]
ledger_bal_alt=[]
csv_bal=[]

for cd, cv in zip(balances['Date'],balances[clargs.account]):
	if cv != cv: # NaN
		continue
	qs = 'select date, balance where account~"{0}" and date<=DATE("{1}")'.format(clargs.account,cd)
	qr=query.run_query(entries,config,qs,()) # ,numberify=True)
	ledger_val=0
	pos = qr[1][-1][1].get_positions()
	if pos:	
		ledger_val = pos[0][0][0]	
#	tvals = [t[1][0][0] for t in qr[1]]
	print(cd,ledger_val,cv,ledger_val-Decimal(cv))
	dates.append(dt.strptime(cd,"%m/%d/%Y"))
	ledger_bal.append(Decimal(ledger_val))
	csv_bal.append(Decimal(cv))

plt.plot(dates,csv_bal,marker='x',label="csv")
plt.plot(dates,ledger_bal,marker='o',label="ledger")
plt.legend()
plt.show()
