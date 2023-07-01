# bcr - bean count report

from beancount.query import query
from beancount.loader import load_file
import argparse
import yaml
import sys
from datetime import datetime as dt
from datetime import timedelta
from beancount.core.data import Open
import numpy as np

ap=argparse.ArgumentParser()
ap.add_argument('-f','--filename',required=True,help='Beancount ledger file')
ap.add_argument('-sd','--start_date',required=False,help='Earliest date (defaults to one year ago)',default=dt.date((dt.now()-timedelta(365))).isoformat())
ap.add_argument('-ed','--end_date',required=False,help='Latest date (defaults to today)',default=dt.date(dt.now()).isoformat())
ap.add_argument('-a','--account',required=False,help='Account regex for reporting (default is all expenses)',default='Expenses')
ap.add_argument('-c','--config',required=False,help='yaml file of accounts for configuring report')
ap.add_argument('-pl','--print_ledger',required=False,action='store_true',default=False,help='Print ledger entries for this filter')
ap.add_argument('-z','--zero_entries',required=False,action='store_true',default=False,help='Print results that sum to 0, otherwise suppress')
ap.add_argument('-ma','--monthly_ave',required=False,action='store_true',default=False,help='Average by month based on start, end dates')

pargs=ap.parse_args(sys.argv[1:])

entries,errors,config=load_file(pargs.filename)

qs="SELECT account, sum(position) FROM OPEN ON {0} CLOSE ON {1} WHERE account ~ '{2}' GROUP BY account ORDER BY account".format(pargs.start_date,pargs.end_date,pargs.account)

qr=query.run_query(entries,config,qs,(),numberify=True)
query_results={}
for r in qr[1]:
	query_results[r[0]]=r[1]

report_accounts={}
if pargs.monthly_ave:
	months_ave=(dt.fromisoformat(pargs.end_date)-dt.fromisoformat(pargs.start_date)).days/30.5
else:
	months_ave=1

if pargs.config:
	with open(pargs.config,'r') as f:
		report_accounts=yaml.safe_load(f)
else:
	# report on all accounts by default
	account_keys=[e.account for e in entries if type(e)==Open]
	account_keys.sort()
	for k in account_keys:
		report_accounts[k]='S'

level=1 # second token in a:b:... format split on :
# NOTE: Assumes accounts are in sort order!
toks=[a.split(':') for a in report_accounts.keys()]
report_groups={}
ci=0
while ci < len(toks):
	g=toks[ci][level] # current group name
	report_groups[g]=[]
	gi=0
	for gt in toks[ci:]:
		if len(gt)>=level:
			if gt[level]==g:
				gi+=1
				report_groups[g].append(':'.join(gt))
		else:
			break
	ci+=gi

report_table={}

for k in report_groups:
	v=0
	for a in report_groups[k]:
		if a in report_accounts and report_accounts[a]=='S' and a in query_results and query_results[a]: # include!
	 		v+=query_results[a]
	report_table[k]=float(v)/months_ave

tot=0
for a in report_table:
	v=float(report_table[a])
	tot+=v
	if v!=0 or pargs.zero_entries:  
		print("{0}\t{1:.2f}".format(a,v))
print("\nTOTAL\t{0:.2f}".format(tot))


# print out legder entries if asked
if pargs.print_ledger: 
	cols={'date':0,'narration':1,'account':2,'position':3}
	qs="SELECT "+','.join(cols.keys())+" FROM OPEN ON {0} CLOSE ON {1} WHERE account ~ '{2}' ORDER BY account,date".format(pargs.start_date,pargs.end_date,pargs.account)

	qr=query.run_query(entries,config,qs,(),numberify=True)
	for r in qr[1]:
		print('\t'.join([str(r[cols[k]]) for k in cols]))
	
