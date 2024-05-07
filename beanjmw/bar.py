# bar.py: Beancount Asset Report
# Input is a tsv file with at least Account, Units, Currency, report_currency
# columns (where report_currency is usually 'USD')
# Generate this file from the Beancount ledger using bcr (Beancount Report)
# with -t Assets and filtered with a bcr.tsv file
#
# Aggregates holdings by e.g. accounts, classes, holdings, etc
# Shows fees (expense ratios, sales load, etc.) for mutual funds
# TODO: Add tax status information
# 
from . import dissect as ds
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import argparse 
import sys
from beancount.loader import load_file
from datetime import datetime as dt
from beancount.core.data import Price, Amount, Decimal
import re

ap=argparse.ArgumentParser()
ap.add_argument('-f','--filename',required=True,help='Assets (tsv, Account,Units,Currency,Report_Currency(default:USD) columns')
ap.add_argument('-af','--advisor_fee',required=False,help='Percent Fee for AUM (for an advisor), default 0',default=0)
ap.add_argument('-tc','--top_holding_cutoff',required=False,help='Perc cutoff for by holding table (default 75)',default=75)
ap.add_argument('-pf','--price_file',required=False,help='Latest price directive file',default='')
ap.add_argument('-rc','--report_currency',required=False,help='Report currency (default USD)',default='USD')
ap.add_argument('-qd','--quote_date',required=False,help='Date to use for quotes (default is today)',default=dt.date(dt.now()).isoformat())

pargs=ap.parse_args(sys.argv[1:])

entries=[]
try:
	if len(pargs.price_file) > 0:
		entries, errors, config = load_file(pargs.price_file)
except:
	sys.stderr.write('Could not load file {0}\n'.format(pargs.price_file))
	entries = []

price_table = ds.create_price_table(entries)

yrs_held=7 # years for sales load to amortize

# at least two columns present, 'Units' and 'Currency'
holdings=pd.read_csv(pargs.filename,sep='\t')

symbol_info={}

# update if cache is this many days out of date
ds.cache_timeout_days=7 

by_account={}

date_qd = dt.date(dt.fromisoformat(pargs.quote_date))

for acct, symbol, units, holding_amt in zip(holdings['Account'], holdings['Currency'],holdings['Units'],holdings[pargs.report_currency]):
	# update price table with implied prices
	# If no price table is supplied, assume the provided values
	# imply a quote price for whatever quote_date is
	# If a price table is supplied, get quotes from there (or failing that,
	# the ds.quote function will try to get an online quote for quote_date)
	if holding_amt > 0 and units > 0 and not len(pargs.price_file)>0:
		if not symbol in price_table:
			price_table[symbol]={}
		price_table[symbol][date_qd]=Price({},date_qd,symbol,Amount(round(Decimal(holding_amt/units),5),pargs.report_currency))
	if not symbol in symbol_info:
		if symbol == pargs.report_currency: # just cash
			symbol_info[symbol]={}
			symbol_info[symbol]['QUOTE']=1.
			symbol_info[symbol]['QUOTE_TYPE']='CASH'
			symbol_info[symbol]['CAT']='CASH'
			symbol_info[symbol]['YF_TABLES']={}
			symbol_info[symbol]['SECT']={}
			symbol_info[symbol]['DIV']=0
			symbol_info[symbol]['ER']=0
			symbol_info[symbol]['SL']=0
			symbol_info[symbol]['DL']=0
			symbol_info[symbol]['HOLDINGS']={}
			symbol_info[symbol]['HOLDINGS']['name']=[pargs.report_currency]
			symbol_info[symbol]['HOLDINGS']['perc']=[100.]
		else:
			symbol_info[symbol] = ds.get_all(symbol,clobber=False,prices=price_table,quote_date=pargs.quote_date)
	amt = units*symbol_info[symbol]['QUOTE']
	if 'QUOTE_CURRENCY' in symbol_info[symbol]:
		amt = amt*ds.get_exchange_rate(symbol_info[symbol]['QUOTE_CURRENCY'], pargs.report_currency)
	if 'TOTAL' in symbol_info[symbol]:
		symbol_info[symbol]['TOTAL']+=amt
	else:
		symbol_info[symbol]['TOTAL']=amt
	if acct in by_account:
		by_account[acct]+=amt
	else:
		by_account[acct]=amt


# Now generate some reporting!

if len(by_account)==0: # nothing to report!
	sys.stderr.write("No accounts in asset list\n")
	sys.exit()

ba_tot=sum([v for v in by_account.values()])
acct_len=max([int(len(k)) for k in by_account])
fmt="{0:<"+str(acct_len)+"s}\t{1:8.2f}\t{2:3.2f}"
print('\n'+'\t'.join(['Account','Total$','Total%']))
# sort descending
sidx = np.argsort(list(by_account.values()))[::-1]
keys=list(by_account.keys())
for i in sidx:
	ba=keys[i]
	print(fmt.format(ba,by_account[ba],100*by_account[ba]/ba_tot))

basic_info={'Stocks':0,'Bonds':0,'Cash':0,'Real Estate':0}

# normalization
gt=0
gt_AUM=0 # under advisor fee
s_tot=[]
h_tot=[]

for s in symbol_info:
	gt+=symbol_info[s]['TOTAL']
	# FIXME: Assumes all non-cash is subject to advisor fee
	if s!=pargs.report_currency:
		gt_AUM+=symbol_info[s]['TOTAL']
	if 'YF_TABLES' in symbol_info[s] and 'Sector Weightings (%)' in symbol_info[s]['YF_TABLES']:
		sw=symbol_info[s]['YF_TABLES']['Sector Weightings (%)'].values()
		s_tot.append(sum([float(v.replace('%','')) for v in sw if v!='N/A']))
	else:
		s_tot.append(sum([v for v in symbol_info[s]['SECT'].values()]))
	if 'CAT' in symbol_info[s]:
		cat = symbol_info[s]['CAT']
	else:
		cat = "UNK"
	if 'YF_TABLES' in symbol_info[s] and 'Overall Portfolio Composition (%)' in symbol_info[s]['YF_TABLES']:
		ctab=symbol_info[s]['YF_TABLES']['Overall Portfolio Composition (%)']
		tot=0
		for c in ['Stocks','Bonds']:
			frac = 0
			if c in ctab:
				frac = float(ctab[c].replace("%",''))/100.0
			tot += frac 
			basic_info[c] += frac*symbol_info[s]['TOTAL']
		basic_info['Cash'] += (1-tot)*symbol_info[s]['TOTAL']
	elif 'QUOTE_TYPE' in symbol_info[s]:
		if symbol_info[s]['QUOTE_TYPE']=='EQUITY':
			c='Stocks'
		elif symbol_info[s]['QUOTE_TYPE']=='MONEYMARKET':
			c='Cash'
		elif symbol_info[s]['QUOTE_TYPE']=='CASH':
			c='Cash'
		else:
			if cat=='Real Estate':
				c='Real Estate'
		basic_info[c]+=symbol_info[s]['TOTAL']

#	print(s, s_tot[-1])
	if 'HOLDINGS' in symbol_info[s]:
		try:
			h_tot.append(sum([v for v in symbol_info[s]['HOLDINGS']['perc']]))
		except:
			for v in symbol_info[s]['HOLDINGS']['perc']:
				if type(v)==str:
					sys.stderr.write('Problem with {0}: {1}\n'.format(s,v))

# print(basic_info)
bi_tot=sum([v for v in basic_info.values()])
print('\n'+'\t'.join(['Class','Total$','Total%']))
for bi in basic_info:
	print("{0:7<s}\t{1:8.2f}\t{2:3.2f}".format(bi,basic_info[bi],100*basic_info[bi]/bi_tot))

#plt.plot(s_tot,label='sector')
#plt.plot(h_tot,label='holdings')
#plt.legend()
#plt.show()

perc=[]
for s in symbol_info:
	perc.append(100*symbol_info[s]['TOTAL']/gt)

# sort high to low by symbol
pidx = np.argsort(perc)
symbol_highlow = np.array([k for k in symbol_info.keys()])[pidx][::-1]

# weighted by holding fraction
w_DIV=0
w_ER=0
w_SL=0
w_DL=0

print('\n'+'\t'.join(['Symbol','Total$','Total%','Div','ER','SL','DL','CAT']))

for s in symbol_highlow:
	cols=[]
	cols.append(s)
	cols.append("{:10.2f}".format(symbol_info[s]['TOTAL']))
	frac_tot=symbol_info[s]['TOTAL']/gt
	cols.append("{:2.2f}".format(100*frac_tot))
	cols.append("{:2.3f}".format(symbol_info[s]['DIV']))
	w_DIV += frac_tot*symbol_info[s]['DIV']
	cols.append("{:2.3f}".format(symbol_info[s]['ER']))
	w_ER += frac_tot*symbol_info[s]['ER']
	cols.append("{:2.3f}".format(symbol_info[s]['SL']))
	w_SL += frac_tot*symbol_info[s]['SL']
	cols.append("{:2.3f}".format(symbol_info[s]['DL']))
	w_DL += frac_tot*symbol_info[s]['DL']
	if 'CAT' in symbol_info[s]:
		cols.append(symbol_info[s]['CAT'])
	else:
		cols.append(symbol_info[s]['QUOTE_TYPE'])
	print('\t'.join(cols))

print('\n'+'\t'.join(['Info','Value']))

print("Grand Total\t{:9.2f}".format(gt))
print("Weighted Div\t{:9.2f}".format(w_DIV))
print("Weighted ER\t{:9.2f}".format(w_ER))
print("Weighted SL\t{:9.2f}".format(w_SL))
print("Weighted DL\t{:9.2f}".format(w_DL))
print("4% Rule Monthly\t{:9.2f}".format(gt*0.04/12))
print("3% Rule Monthly\t{:9.2f}".format(0.03*gt/12))
print("2% Rule Monthly\t{:9.2f}".format(0.02*gt/12))
print("Est Annual Div (pre-tax)\t{:9.2f}".format(gt*w_DIV*0.01))

by_fees={}
by_fees['Est Annual Advisor Fee']=0.01*gt_AUM*float(pargs.advisor_fee)
by_fees['Est Annual Sales Load']=0.01*gt*w_SL/yrs_held
by_fees['Est Annual Expense Ratio']=0.01*gt*w_ER

print("\nFee breakdown:")
for f in by_fees:
	print("{0}\t{1:9.2f}".format(f,by_fees[f]))
print("Total Monthly Fees\t{0:7.2f}".format(sum([v for v in by_fees.values()])/12.))

# plot by sector

by_sector={}

for s in symbol_highlow:
	frac_tot=symbol_info[s]['TOTAL']/gt
	if 'Sector Weightings (%)' in symbol_info[s]['YF_TABLES']:
		stab =symbol_info[s]['YF_TABLES']['Sector Weightings (%)']
	else:
		stab = symbol_info[s]['SECT']
	for sect in stab:
		v = stab[sect]
		if type(v) == str:
			v = float(stab[sect].replace('%','').replace('N/A','0')) 
		if sect in by_sector:
			by_sector[sect]+=(frac_tot*v)
		else:
			by_sector[sect]=(frac_tot*v)

kidx = np.argsort([v for v in by_sector.values()])[::-1]
sector_highlow = np.array([k for k in by_sector.keys()])[kidx]

print('\n'+'Sector\tPercent')
flen = max([len(x) for x in sector_highlow])
fmt = "{0:<"+str(flen+1)+"}\t{1:3.2f}"
for s in sector_highlow:
	print(fmt.format(s,by_sector[s]))	

print(sum([v for v in by_sector.values()]))

# plot by holdings

by_holding={}

# attempt to reduce near redundancies
clean_holdings='\t'
clean_holdings_list=[]

equivalent_suffix = [' INC',' & CO',' CO ',' CO$',' CO-',' CORP',' LLC',' LTD',' PLC',' AG ',' AG$',' COMPAN']

rep_chars = {
	'/':' ',
	'.':' ',
	'\t':'',
	'(':' ',
	'*':' ',
	')':' ',
}

def clean_holding(s):
	trunc_s = s.upper()
	for r in rep_chars:
		trunc_s = trunc_s.replace(r,rep_chars[r])
	for es in equivalent_suffix:
		if es in trunc_s:
			trunc_s = trunc_s.split(es)[0]+" CORP"
	return(trunc_s)

for s in symbol_highlow:
	frac_tot=symbol_info[s]['TOTAL']/gt
	perc_by_name={}
	# create dict; note some names are degenerate
	for h,p in zip(symbol_info[s]['HOLDINGS']['name'],symbol_info[s]['HOLDINGS']['perc']):
		clean_h = clean_holding(h)
		sr = re.search(clean_h,clean_holdings)
		if not sr:
			# separate names with tabs
			clean_holdings_list.extend(clean_h + '\t')
			clean_holdings = ''.join(clean_holdings_list)
		else: # matched a portion of a longer name
			full_holding = re.search('\t[^\t]*'+clean_h+'.*?\t',clean_holdings)
			if full_holding:
				clean_h = full_holding.group()[1:-1]
			else: # this is an error
				sys.stderr.write("No long match for {0}\n".format(clean_h))
			
		if clean_h in perc_by_name:
			perc_by_name[clean_h]+=p
		else:
			perc_by_name[clean_h]=p

	for h in perc_by_name:
		if h in by_holding:
			by_holding[h]+=(frac_tot*perc_by_name[h])
		else:
			by_holding[h]=(frac_tot*perc_by_name[h])

kidx = np.argsort([v for v in by_holding.values()])[::-1]
holding_highlow = np.array([k for k in by_holding.keys()])[kidx]

flen = max([len(x) for x in holding_highlow])
fmt = "{0:<"+str(flen+1)+"}\t{1:3.4f}"
cumulative=0
print('\n'+"Instrument\tHolding%")
n_top=0
for s in holding_highlow:
	print(fmt.format(s,by_holding[s]))	
	cumulative+=by_holding[s]
	n_top+=1
	if cumulative > float(pargs.top_holding_cutoff):
		break
print(fmt.format("Remainder({0})".format(int(len(holding_highlow))-n_top),100-cumulative))

print("\nPercent Total Holdings\t{0:3.2f} ({1} in top {2:3.1f}%)".format(sum([v for v in by_holding.values()]),n_top,float(pargs.top_holding_cutoff)))
