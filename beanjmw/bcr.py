# bcr - bean count report

from beancount.query import query
from beancount.loader import load_file
import argparse
import os
import sys
from datetime import datetime as dt
from datetime import timedelta
from beancount.core.data import Open
import numpy as np
from collections import namedtuple

# for report_config file
report_config_default = 'bcr.tsv'
reportFields=['account','include','months','summarize']
ReportElement=namedtuple('ReportElement',reportFields) 

# one of these at a time
top_level=['Assets','Liabilities','Income','Expenses']

ap=argparse.ArgumentParser()
ap.add_argument('-f','--filename',required=True,help='Beancount ledger file')
ap.add_argument('-sd','--start_date',required=False,help='Earliest date (defaults to one year ago)',default=dt.date((dt.now()-timedelta(365))).isoformat())
ap.add_argument('-ed','--end_date',required=False,help='Latest date (defaults to today)',default=dt.date(dt.now()).isoformat())
ap.add_argument('-t','--type',required=False,help='Top-level type, one of: Assets, Expenses (default), Income, Liabilities',default='Expenses')
ap.add_argument('-a','--account',required=False,help='Account regex for reporting (default is all in top-level type)',default='')
ap.add_argument('-rc','--report_config',required=False,help='Report tsv config file',default=report_config_default)
ap.add_argument('-pl','--print_ledger',required=False,action='store_true',default=False,help='Print ledger entries for this filter')
ap.add_argument('-z','--zero_entries',required=False,action='store_true',default=False,help='Print results that sum to 0, otherwise suppress')
ap.add_argument('-ma','--monthly_ave',required=False,action='store_true',default=False,help='Average by month based on start, end dates')
ap.add_argument('-dt','--details',required=False,action='store_true',default=False,help='Print subtotals, number of entries, and average months for each top-level entry')
ap.add_argument('-cl','--clobber',required=False,action='store_true',default=False,help='Overwrite default config file if set')

pargs=ap.parse_args(sys.argv[1:])

entries,errors,config=load_file(pargs.filename)

acct_match=pargs.type
if len(pargs.account) > 0:
	acct_match==pargs.account

qs="SELECT account, sum(position), count(position) FROM OPEN ON {0} CLOSE ON {1} WHERE account ~ '{2}' GROUP BY account ORDER BY account".format(pargs.start_date,pargs.end_date,acct_match)

qr=query.run_query(entries,config,qs,(),numberify=True)
query_results={}
for r in qr[1]:
	query_results[r[0]]=[r[1],r[2]] # amount, count, by account as key
	if pargs.details:
		print(r)

report_accounts=[]
if pargs.monthly_ave:
	months_ave=(dt.fromisoformat(pargs.end_date)-dt.fromisoformat(pargs.start_date)).days/30.412
else:
	months_ave=1

# config file is tsv columns, with columns defined by ReportElement fields
if pargs.report_config and os.path.isfile(pargs.report_config):
	sys.stderr.write("Reading config {0}...\n".format(pargs.report_config))
	table=[]
	with open(pargs.report_config,'r') as f:
		for l in f.readlines():
			ls=l.strip()
			if len(ls)==0:
				continue
			if ls[0]=='#':
				continue
			else:
				table.append(ls.split('\t'))

	for e in map(ReportElement._make,table):
		report_accounts.append(e)
	sys.stderr.write("{0} accounts in {1}\n".format(len(report_accounts),pargs.report_config))
else:
	# report on all accounts by default
	account_keys=[e.account for e in entries if type(e)==Open]
	account_keys.sort()
	for k in account_keys:
		report_accounts.append(ReportElement(account=k,include=True,months=0,summarize=True))
	# now save a copy
	if os.path.isfile(report_config_default) and not pargs.clobber:
		sys.stderr.write("{} exists - use -clobber option to overwrite\n".format(report_config_default))
	else:
		with open(report_config_default,'w') as f:
			for e in report_accounts:
				f.write('\t'.join([str(ei) for ei in e])+'\n')

level=1 # second token in a:b:... format split on :
# NOTE: Assumes accounts are in sort order!
report_account_names=[e.account for e in report_accounts if pargs.type in e.account]
toks=[e.account.split(':') for e in report_accounts if pargs.type in e.account]
report_groups={}
ci=0
while ci < len(toks):
	if toks[ci][level] == 'US': # don't group by country
		tlevel=level+1
	else:
		tlevel=level
	g=toks[ci][tlevel] # current group name
	if not g in report_groups:
		report_groups[g]=[]
	gi=0
	for gt in toks[ci:]:
		if len(gt)>tlevel:
			if gt[tlevel]==g:
				gi+=1
				report_groups[g].append(':'.join(gt))
		else:
			break
	ci+=gi

report_table={}

for k in report_groups:
	v=0
	details=[]
	for a in report_groups[k]:
		e = report_accounts[report_account_names.index(a)]
		ma=float(e.months)
		if ma <=0:
			ma = months_ave # default value
		# include!
		if a in query_results and query_results[a] and str(e.include).upper()=='TRUE':
			if query_results[a][0]:
				res=float(query_results[a][0])/ma
			else:
				res=0
			details.append((a,"{0:.2f}".format(res),"{0:.2f}".format(ma),query_results[a][1]))
			v+=res
	report_table[k]=(v,details)

tot=0
for a in report_table:
	v=report_table[a][0]
	tot+=v
	if v!=0 or pargs.zero_entries:  
		print("{0}\t{1:.2f}".format(a,v))
		if pargs.details:
			for st in report_table[a][1]:
				print('\t'+'\t'.join([str(x) for x in st]))
print("\nTOTAL\t{0:.2f}".format(tot))


# print out legder entries if asked
if pargs.print_ledger: 
	cols={'date':0,'narration':1,'account':2,'position':3}
	qs="SELECT "+','.join(cols.keys())+" FROM OPEN ON {0} CLOSE ON {1} WHERE account ~ '{2}' ORDER BY account,date".format(pargs.start_date,pargs.end_date,pargs.account)

	qr=query.run_query(entries,config,qs,(),numberify=True)
	for r in qr[1]:
		print('\t'.join([str(r[cols[k]]) for k in cols]))
