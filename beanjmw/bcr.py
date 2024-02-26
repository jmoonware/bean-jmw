# bcr - bean count report

from beancount.query import query
from beancount.loader import load_file
from beancount.parser import printer
import argparse
import os
import sys
import re
from datetime import datetime as dt
from datetime import timedelta
from beancount.core.data import Open, Amount, Decimal
from beancount.core.data import Price
from operator import attrgetter
import numpy as np
from collections import namedtuple
import matplotlib.pyplot as plt
from . import dissect as ds

report_currency='USD'
def convert_currency(symbol,units,quote_date=None):
	res = round(Decimal(units),5)
	if symbol!=report_currency:
		prc=ds.quote(symbol, prices=price_table,quote_date=quote_date)
		ef = 1
		if prc.amount.currency!=report_currency:
			ef = ds.get_exchange_rate(prc.amount.currency, report_currency)
		res = round(Decimal(ef),5)*res*prc.amount.number
	return(res)

# for report_config file
report_config_default = 'bcr.tsv'
reportFields=['account','include','months','taxfed','taxstate']
ReportElement=namedtuple('ReportElement',reportFields) 

# one of these at a time
top_level=['Assets','Liabilities','Income','Expenses']
print_totals=False # turn on for expenses by default

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
ap.add_argument('-pt','--print_totals',required=False,action='store_true',default=False,help='Print grand totals by currency at end')
ap.add_argument('-z','--zero_entries',required=False,action='store_true',default=False,help='Print results that sum to 0, otherwise suppress')
ap.add_argument('-ma','--monthly_ave',required=False,action='store_true',default=False,help='Average by month based on start, end dates')
ap.add_argument('-dt','--details',required=False,action='store_true',default=False,help='Print subtotals, number of entries, and average months for each top-level entry')
ap.add_argument('-cl','--clobber',required=False,action='store_true',default=False,help='Overwrite default config file if set')
ap.add_argument('-html','--html',required=False,action='store_true',default=False,help='Output in static html')
ap.add_argument('-np','--no_plot',required=False,action='store_true',default=False,help='No interactive plot - just print and exit')
ap.add_argument('-pf','--price_file',required=False,default='prices.txt',help='Beancount price directive file to use')
ap.add_argument('-cf','--css_file',required=False,default='bcr.css',help='Beancount report css file - default is "bcr.css", will be copied over if missing')
ap.add_argument('-ea','--expand_all',required=False,default=False,action='store_true',help='Expand all details tables by default')

clargs=ap.parse_args(sys.argv[1:])

if not clargs.type in ['Assets','Liabilities','Income','Expenses','Equity']:
	sys.stderr.write("Invalid type {0}\n".format(clargs.type))
	sys.exit()

entries,errors,config=load_file(clargs.filename)
# first thing - create price table
if os.path.exists(clargs.price_file):
	price_entries, errors, config = load_file(clargs.price_file)
	price_table=ds.create_price_table(price_entries)
else:
	price_table=ds.create_price_table(entries)

# create a table of the commodity directives by currency
commodity_table={}
ds.create_commodity_table(entries,commodity_table)
excluded_commodities = [commodity_table[c].currency for c in commodity_table if commodity_table[c].meta and "export" in commodity_table[c].meta and commodity_table[c].meta['export'].upper()=="IGNORE"]

if len(excluded_commodities) > 0:
	sys.stderr.write("Excluding commodities {0}\n".format(excluded_commodities)) 

# if size changes, save updated version below
pt_size = ds.size_price_table(price_table)

acct_match=clargs.type
if len(clargs.account) > 0:
	acct_match==clargs.account

qs="SELECT account, sum(position), count(position) FROM OPEN ON {0} CLOSE ON {1} WHERE account ~ '{2}' GROUP BY account ORDER BY account".format(clargs.start_date,clargs.end_date,acct_match)

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
if clargs.monthly_ave:
	months_ave=(dt.fromisoformat(clargs.end_date)-dt.fromisoformat(clargs.start_date)).days/30.412
else:
	months_ave=1

# make a set of open accounts in ledger
account_set=set()
for e in filter(lambda e: type(e)==Open, entries):
	if e.currencies and sum([x in e.currencies for x in excluded_commodities]):
		continue
	account_set.add(e.account)

account_keys=sorted(account_set)

def save_config(path,report_elements):
	accts = [e.account for e in report_accounts]
	idx = np.argsort(accts)
	sorted_re = [report_elements[i] for i in idx]
	max_len = max([int(len(e.account)) for e in report_accounts])
	with open(path,'w') as f:
		fmt='{0:<'+str(max_len)+'s}\t{1}\t{2}\t{3}\t{4}\n'
		for e in sorted_re:
			f.write(fmt.format(e.account,e.include,e.months,e.taxfed,e.taxstate))
	return(sorted_re)

# config file is tsv columns, with columns defined by ReportElement fields
if clargs.report_config and os.path.isfile(clargs.report_config):
	sys.stderr.write("Reading config {0}...\n".format(clargs.report_config))
	table=[]
	with open(clargs.report_config,'r') as f:
		for l in f.readlines():
			ls=l.strip()
			if len(ls)==0:
				continue
			if ls[0]=='#':
				continue
			else:
				table.append([x.strip() for x in ls.split('\t')])

	config_accounts=set()
	for e in map(ReportElement._make,table):
		report_accounts.append(e)
		config_accounts.add(report_accounts[-1].account)

	# now check for new accounts in ledger
	num_config=len(report_accounts)
	for le in account_keys:
		if not le in config_accounts:
			report_accounts.append(ReportElement(le,'y',0,'y','y'))
	if len(report_accounts) > num_config: # new accounts found 
		report_accounts = save_config(clargs.report_config,report_accounts)
	sys.stderr.write("{0} accounts ({1} new) in {2}\n".format(len(report_accounts),len(report_accounts)-num_config,clargs.report_config))
else:
	# report on all accounts by default, already sorted above
	for k in account_keys:
		report_accounts.append(ReportElement(k,'y',0,'y','y'))
	# now save a copy
	if os.path.isfile(report_config_default) and not clargs.clobber:
		sys.stderr.write("{} exists - use -clobber option to overwrite\n".format(report_config_default))
	else:
		report_accounts = save_config(report_config_default,report_accounts)

level=int(clargs.level) # level'th token in a:b:... format split on :
# take out excluded accounts 
filtered_report_accounts=[e for e in report_accounts if re.match(clargs.type,e.account) and str(e.include).upper()=='Y']
# take out ignored commodities too 
report_account_names=[e.account for e in filtered_report_accounts]
toks=[e.account.split(':') for e in filtered_report_accounts]

# needed for printing below
if len(filtered_report_accounts)==0:
	sys.stderr.write("Nothing to report for {0}, {1}\n".format(clargs.type,clargs.account))
	sys.exit()
else:
	max_account_len = max([len(e.account) for e in filtered_report_accounts])

report_groups={}
ci=0
skip_levels=['US','UK','Federal','State']
while ci < len(toks):
	clevel=level
	if clevel >= len(toks[ci]):
		sys.stderr.write("Warning: Level {0} exceeds depth of {1}\n".format(clevel,':'.join(toks[ci])))
		clevel=len(toks[ci])-1
	if toks[ci][clevel] in skip_levels: # don't group by these
		tlevel=clevel+1
	else:
		tlevel=clevel
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

# use values as of this date to convert to report currency
quote_date=dt.date(dt.fromisoformat(clargs.end_date))

for k in report_groups:
	v={} # by currency
	v_rc={} # everything converted to the report currency
	currency = ''
	details=[]
	for a in report_groups[k]:
		e = filtered_report_accounts[report_account_names.index(a)]
		ma=float(e.months)
		if ma <=0:
			ma = months_ave # default value
		if a in query_results and query_results[a]:
			# get the Inventory...
			positions=query_results[a][0].get_positions()
			if len(positions) == 0:
#				sys.stderr.write("Warning: no positions in {0} for {1}\n".format(query_results[a],a))
				amount=Amount(Decimal('0'),report_currency)
			else:
				tot=sum([pi[0][0] for pi in positions])
				pcur = np.unique([a[0][1] for a in positions])
				if len(pcur) > 1:
					sys.stderr.write("Warning: Multiple currencies in position {0} {1}\n".format(pcur,a))
				amount = Amount(tot,pcur[0])
			currency=amount[1]
			if not currency in v:
				v[currency]=Decimal('0.00000')
			if not currency in v_rc:
				v_rc[currency]=Decimal('0.00000')
			if amount[0]:
				res = abs(amount[0]/round(Decimal(ma),5))
				res_rc = convert_currency(currency, res, quote_date)
			else:
				res=Decimal('0.00000')
				res_rc=Decimal('0.00000')
			details.append((a,res,ma,query_results[a][1],res_rc))
			v[currency]+=res
			v_rc[currency]+=res_rc
	report_table[k]=(v, v_rc, details)

print_doc=[]
# Sigh. It is 2023, still writing print statements of shitty HTML...
if clargs.html:
	# make sure we have css file
	if not os.path.isfile(clargs.css_file):
		import beanjmw
		mod_path = beanjmw.__path__[0]
		copy_file = os.path.join(mod_path,clargs.css_file)
		if os.path.isfile(copy_file):
			os.system("cp {0} .".format(copy_file))
		else:
			system.stderr.write("Can't find {0} to copy\n".format(copy_file))

	print_doc.append("<!DOCTYPE html>")
	print_doc.append("<html>")
	print_doc.append('<head>')
	print_doc.append('<link rel="stylesheet" href="{0}" >'.format(clargs.css_file))
	print_doc.append("</head>")
	print_doc.append("<body>")
	subcatfmt='<tr class="subcat"><td><label for="t{0}"/></td><td>{1}</td><td>{2:.2f}</td><td>{3}</td><td>{4}</td><td><b>{5:1.2f}</b></td></tr>'
	rowhidefmt='<tr class="rowhide"><td> </td><td>{0}</td><td>{1}</td><td></td><td>{2}</td></tr>'
	tfmt='<tr><td>{0}</td><td>{1:.2f}</td><td>{2}</td><td>{3:.2f}</td></tr>'
	print_doc.append("<h1>{0} Report {1}</h1>".format(clargs.type,dt.date(dt.now()).isoformat()))
	print_doc.append("<h2>Period from {0} to {1}</h2>".format(clargs.start_date,clargs.end_date))
	if clargs.monthly_ave:
		print_doc.append("<h2>Average Per Month</h2>")
	print_doc.append("<table>")
	print_doc.append("<tr>")
	print_doc.append("<td>")
	print_doc.append("<table>")
	print_doc.append("<tr><th>Account</th><th>Units</th><th>Currency</th><th>{}</th></tr>".format(report_currency))
else: # plain text
	subcatfmt='\t{0:<'+str(max_account_len)+'s}\t{1:7.2f}\t{2:3.2f}\t{3}\t{4:7.2f}'
	rowhidefmt='\t\t{0}\t{1}\t{2}'
	tfmt='{0}\t{1:.2f}\t{2}\t{3:.2f}'
	print_doc.append("Account\tUnits\tCurrency\t{}".format(report_currency))

tot={}
plot_labels=[]
plot_values=[]
for a in report_table:
	v=report_table[a][0]
	v_rc=report_table[a][1]
	for currency in v: 
		if not currency in tot:
			tot[currency]=Decimal('0.00000')
		tot[currency]+=v_rc[currency]
		if v[currency]!=0 or clargs.zero_entries:
			if currency!=report_currency: 
				plot_labels.append(':'.join([a,currency]))
			else:
				plot_labels.append(a)
			plot_values.append(v_rc[currency])
			print_doc.append(tfmt.format(a,v[currency],currency,v_rc[currency]))

if clargs.html:
	print_doc.append("</table>")
	print_doc.append("</td>")

fig, ax = plt.subplots()
fig.set_size_inches(5,7)
idx = np.argsort(plot_values)[::-1]
if clargs.type=='Assets':
	mult=1e-3
	x_tag=' k'
else:
	mult=1
	x_tag=''
ax.barh(range(len(plot_values)),mult*np.array(plot_values,dtype=float)[idx])
ax.set_yticks(range(len(plot_values)),labels=np.array(plot_labels)[idx])
ax.invert_yaxis()
ax.set_xlabel("Value ({0}{1}) ".format(report_currency,x_tag))
if clargs.monthly_ave:
	ax.set_title("{0}: {1:,.0f} {2}/mo ({3:,.0f} tot)".format(clargs.type,sum(plot_values),report_currency,months_ave*float(sum(plot_values))))
else:
	ax.set_title("Total {0}: {1:,.0f} {2}".format(clargs.type,sum(plot_values),report_currency))
plt.tight_layout()
if clargs.html:
	fn=os.path.split(os.path.splitext(clargs.filename)[0])[-1]+'_'+clargs.type+'.png'
	plt.savefig(fn)
	print_doc.append("<td>")
	print_doc.append('<img src="{0}">'.format(fn))
	print_doc.append("</td>")
	print_doc.append("</tr>")
	print_doc.append("</table>")
else:
	if not clargs.no_plot:
		plt.show()

if clargs.print_totals:
	gt=0
	for c in tot:
		gt+=tot[c]
		print_doc.append("TOTAL {0}\t{1:.2f}".format(c,tot[c]))
	print_doc.append("---------")
	print_doc.append("{0}\t{1}\tGrandTotal{2}\t{3:.2f}".format(clargs.start_date,clargs.end_date,clargs.type,gt))
 
# details tables
max_narration=50 # characters long, or pad to this value
details_count=0
if clargs.details:
	if clargs.html:
		print_doc.append('<div class="pagebreak"></div>')
		print_doc.append('<table>')
		print_doc.append('<tr class="subcat"><th></th><th>Account</th><th>Amount</th><th></th><th>Entries</th><th>Total</th></tr>')
		topcatfmt = '<tr class="topcat"><th></th><th>{0}</th><td></td><td></td><td></td><th>{1:.2f}</th></tr>'
	else:
		topcatfmt = '{0}\t\t\t\t{1:.2f}'
	for a in report_table:
		if len(report_table[a][2]) > 0:
			tot=0
			for st in report_table[a][2]:
				tot+=st[4]
			print_doc.append(topcatfmt.format(a,tot))
		for st in report_table[a][2]:
			if clargs.html: # details table within larger table
				print_doc.append(subcatfmt.format(details_count,st[0],st[1],'',st[3],st[4]))
				print_doc.append("<tr><td colspan=6>")
				if clargs.expand_all:
					print_doc.append('<input type="checkbox" id="t{0}"/>'.format(details_count))
				else:
					print_doc.append('<input type="checkbox" id="t{0}" checked/>'.format(details_count))
				details_count+=1
				print_doc.append('<div class="hide">')
				print_doc.append('<table>')
			else:
				print_doc.append(subcatfmt.format(st[0],st[1],'',st[3],st[4]))
			# re-query to get actual ledger entries
			qs="SELECT date, narration, change FROM OPEN ON {0} CLOSE ON {1} WHERE account ~ '{2}$' ORDER BY date".format(clargs.start_date,clargs.end_date,st[0])
			qr=query.run_query(entries,config,qs,()) 
			if len(qr[1]) > 0:
				print_doc.append(rowhidefmt.format("Date","Narration","Change"))
			for r in qr[1]:
				narr_str=r.narration
				if len(r.narration) < max_narration:
					narr_str = r.narration+' '*(max_narration-len(r.narration))
				print_doc.append(rowhidefmt.format(r.date,narr_str[:max_narration],r.change[0][0]))
			if clargs.html:
				print_doc.append("</table></div></td></tr>")

	if clargs.html:
		print_doc.append('</table>')
		

# print out ledger entries if asked
if clargs.print_ledger: 
	cols={'date':0,'narration':1,'account':2,'position':3}
	qs="SELECT "+','.join(cols.keys())+" FROM OPEN ON {0} CLOSE ON {1} WHERE account ~ '{2}' ORDER BY account,date".format(clargs.start_date,clargs.end_date,clargs.account)

	qr=query.run_query(entries,config,qs,()) 
	for r in qr[1]:
		print_doc.append('\t'.join([str(r[cols[k]]) for k in cols]))

if clargs.html:
	print_doc.append("</body>")
	print_doc.append("</html>")

# save the price table
if pt_size!=ds.size_price_table(price_table):
	ds.save_price_table(clargs.price_file,price_table)

print('\n'.join(print_doc))
