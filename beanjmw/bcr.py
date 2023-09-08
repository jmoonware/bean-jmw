# bcr - bean count report

from beancount.query import query
from beancount.loader import load_file
import argparse
import os
import sys
import re
from datetime import datetime as dt
from datetime import timedelta
from beancount.core.data import Open, Amount, Decimal
import numpy as np
from collections import namedtuple

# for report_config file
report_config_default = 'bcr.tsv'
reportFields=['account','include','months','summarize']
report_currency='USD'
ReportElement=namedtuple('ReportElement',reportFields) 

# one of these at a time
top_level=['Assets','Liabilities','Income','Expenses']

ap=argparse.ArgumentParser()
ap.add_argument('-f','--filename',required=True,help='Beancount ledger file')
ap.add_argument('-sd','--start_date',required=False,help='Earliest date (defaults to one year ago)',default=dt.date((dt.now()-timedelta(365))).isoformat())
ap.add_argument('-ed','--end_date',required=False,help='Latest date (defaults to today)',default=dt.date(dt.now()).isoformat())
ap.add_argument('-t','--type',required=False,help='Top-level type, one of: Assets, Expenses (default), Income, Liabilities',default='Expenses')
ap.add_argument('-l','--level',required=False,help='Report this level of account tree (usually 1 by default)',default=1)
ap.add_argument('-a','--account',required=False,help='Account regex for reporting (default is all in top-level type)',default='')
ap.add_argument('-rc','--report_config',required=False,help='Report tsv config file',default=report_config_default)
ap.add_argument('-c','--currency',required=False,help='Report in this currency (default USD)',default=report_currency)
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

# Note that the sum of positions is an Inventory
# An inventory is a set of positions (here, usually one sum)
# A Position is a tuple of Amount and Cost 
# An Amount is a tuple of Decimal units and Currency (string)
# (A Cost is a NamedTuple of number, currency, date, and optional str label)
# So, to get to the decimal units of the Inventory, need to index as
# DecimalUnits = Inventory.get_positions[0][0][0]
qr=query.run_query(entries,config,qs,()) # ,numberify=True)
query_results={}
for r in qr[1]:
	query_results[r[0]]=[r[1],r[2]] # Inventory, count, by account as key

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
				table.append([x.strip() for x in ls.split('\t')])

	for e in map(ReportElement._make,table):
		report_accounts.append(e)
	sys.stderr.write("{0} accounts in {1}\n".format(len(report_accounts),pargs.report_config))
else:
	# report on all accounts by default
	account_keys=[e.account for e in entries if type(e)==Open]
	account_keys.sort()
	for k in account_keys:
		report_accounts.append(ReportElement(account=k,include='y',months=0,summarize='n'))
	# now save a copy
	max_account_len = int(max([len(e.account) for e in report_accounts]))
	if os.path.isfile(report_config_default) and not pargs.clobber:
		sys.stderr.write("{} exists - use -clobber option to overwrite\n".format(report_config_default))
	else:
		with open(report_config_default,'w') as f:
			fmt='{0:<'+str(max_account_len)+'s}\t{1}\t{2}\t{3}\n'
			for e in report_accounts:
				f.write(fmt.format(e.account,e.include,e.months,e.summarize))

level=int(pargs.level) # second token in a:b:... format split on :
# NOTE: Assumes accounts are in sort order!
filtered_report_accounts=[e for e in report_accounts if re.match(pargs.type,e.account) and str(e.include).upper()=='Y']
report_account_names=[e.account for e in filtered_report_accounts]
toks=[e.account.split(':') for e in filtered_report_accounts]

# needed for printing below
max_account_len = max([len(e.account) for e in filtered_report_accounts])

report_groups={}
ci=0
while ci < len(toks):
	if level >= len(toks[ci]):
		sys.stderr.write("Warning: Level {0} exceeds depth of {1}\n".format(level,':'.join(toks[ci])))
		level=len(toks[ci])-1
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
	v={} # by currency
	currency = ''
	details=[]
	for a in report_groups[k]:
		e = filtered_report_accounts[report_account_names.index(a)]
		ma=float(e.months)
		if ma <=0:
			ma = months_ave # default value
		if a in query_results and query_results[a]:
			positions=query_results[a][0].get_positions()
			if len(positions) > 1:
				sys.stderr.write("Warning: multiple positions in {0}\n".format(query_results[a]))
			if len(positions) == 0:
				sys.stderr.write("Warning: no positions in {0} for {1}\n".format(query_results[a],a))
				first_position=(Amount(Decimal('0'),'USD'),None)
			else:
				first_position=positions[0]
			amount = first_position[0] 
			currency=amount[1]
			if not currency in v:
				v[currency]=0.
			if amount[0]:
				res = float(amount[0])/ma
			else:
				res=0.
			details.append((a,res,ma,query_results[a][1]))
			v[currency]+=res
	report_table[k]=(v, details)

tot={}
dfmt='\t{0:<'+str(max_account_len)+'s}\t{1:7.2f}\t{2:3.2f}\t{3}'
for a in report_table:
	v=report_table[a][0]
	for currency in v: 
		if not currency in tot:
			tot[currency]=0.
		tot[currency]+=v[currency]
		if v[currency]!=0 or pargs.zero_entries:  
			print("{0}\t{1:.2f} {2}".format(a,v[currency],currency))
			if pargs.details:
				for st in report_table[a][1]:
					print(dfmt.format(st[0],st[1],st[2],st[3]))
for c in tot:
	print("\nTOTAL {0}\t{1:.2f}".format(c,tot[c]))


# print out legder entries if asked
if pargs.print_ledger: 
	cols={'date':0,'narration':1,'account':2,'position':3}
	qs="SELECT "+','.join(cols.keys())+" FROM OPEN ON {0} CLOSE ON {1} WHERE account ~ '{2}' ORDER BY account,date".format(pargs.start_date,pargs.end_date,pargs.account)

	qr=query.run_query(entries,config,qs,()) # ,numberify=True)
	for r in qr[1]:
		print('\t'.join([str(r[cols[k]]) for k in cols]))
