import requests
import re
import numpy as np
import pandas as pd
# remember to whitelist fc.yahoo.com for yfinance if using pihole!
import yfinance as yf
import sys,os
from datetime import datetime as dt
from datetime import timedelta
from pytz import timezone as tz
import yaml
from bs4 import BeautifulSoup as BS
from beancount.core.data import Price, Amount, Decimal, Commodity
import shutil
from beancount.loader import printer

headers = {
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0"
}


cache_dir='yaml'
cache_timeout_days=5

symbol='VONG' # ETF
symbol='SPMD' # ETF
#symbol='FXAIX' # MF
symbol='AGTHX' # MUTUALFUND
#symbol='ILMN' # EQUITY
#symbol='GOTXX' # MONEYMARKET

mf_url='https://www.zacks.com/funds/mutual-fund/quote/{}'
etf_url='https://www.zacks.com/funds/etf/{}'
equity_url='https://www.zacks.com/stock/quote/{}'
etf_holding_url='/'.join([etf_url,'holding'])
mf_holding_url='/'.join([mf_url,'holding'])
etf_sector_url='https://etfdb.com/etf/{}/#charts'
yf_holding_url='https://finance.yahoo.com/quote/{}/holdings'

def backup_file(file_name):
	if os.path.isfile(file_name):
		v=0
		while os.path.isfile('.'.join([file_name,str(v)])):
			v=v+1
		shutil.copy(file_name,'.'.join([file_name,str(v)]))
	return


def create_commodity_table(entries,commodity_table=None):
	if commodity_table==None:
		commodity_table={}
	for e in filter(lambda e: type(e)==Commodity,entries):
	    if not e.currency in commodity_table:
	        commodity_table[e.currency]=e
	return(commodity_table)

def create_price_table(entries,price_table=None):
	if price_table==None:
		price_table={}
	for e in filter(lambda e: type(e)==Price,entries):
	    if not e.currency in price_table:
	        price_table[e.currency]={}
	    # don't overwrite entries without cache_timeout_days
	    if not e.date in price_table[e.currency] or (e.date in price_table[e.currency] and 'cache_timeout_days' in e.meta):
	        price_table[e.currency][e.date]=e
	# now sort the prices by date, once
	for c in price_table:
	    price_table[c]=dict(sorted(price_table[c].items()))
	return(price_table)

def size_price_table(price_table):
	return(sum([len(price_table[s]) for s in price_table]))

def save_price_table(price_file,price_table):
	backup_file(price_file)
	with open(price_file,'w') as f:                                        
		for symbol in price_table:
			printer.print_entries([price_table[symbol][k] for k in price_table[symbol]],file=f)  
	return
	
# exchange rate URL
exchange_rate_url='https://api.exchangerate-api.com/v4/latest/'

def get_exchange_rate(from_curr,to_curr):
	ef = 1
	if from_curr != to_curr:
		r = requests.get(exchange_rate_url+from_curr.upper())
		data = r.json()
		if data and 'rates' in data:
			rates=data['rates']
			if from_curr.upper() in rates and to_curr in rates:
				ef = rates[to_curr]
				# KLUDGE for pence vs Pound
				if from_curr[-1].islower():
					ef=0.01*ef
			else:
				sys.write.stderr("Can't get exchange rate between {0} and {1}\n".format(from_curr,to_curr))
	return(ef)

def get_page(url):
	r=''
	with requests.Session() as req:
		req.headers.update(headers)
		r = req.get(url)
	return(r)

def get_yahoo(symbol):
	ret={}
	try:
		r = get_page(yf_holding_url.format(symbol))
		soup = BS(r.text,features='lxml')
		tabs = soup.find_all('div',{'class': 'Mb(25px)'}) 
		row_pat = '<span>([a-z /A-Z]+?)</span>'
		data_pat = '<span.*?>([0-9]+\.*[0-9]*%*|\s*N/A\s*)</span>'
		tables=[]
		titles=[]
		for t in tabs:
			tables.append([])
			tit_pat='<h3>(.+?)</h3>'
			m = re.search(tit_pat,str(t))
			if m:
				n = re.search('<span>(.+?)</span>',m.group(1))
				titles.append(n.group(1))
			rows = t.findChildren('div')
			for row in rows:
				els = row.findChildren('span',recursive=False)
				if len(els) > 0:
					m = re.search(row_pat,str(els[0]))
					d = re.search(data_pat,str(els[-1])) 
					if m and d:
						tables[-1].append([m.group(1),d.group(1)])
	
		if len(titles)==len(tables) and len(titles)>0:
			for t,tab in zip(titles,tables):
				ret[t]={}
				for dp in tab:
					ret[t][dp[0]]=dp[1]
	except Exception as ex:
		sys.stderr.write("get_yahoo: can't get info for {0}:{1}\n".format(symbol,ex))
	return(ret)			

# splits on double quote, comma, double quote with optional whitespace
el_pat = '" *, *"'

replace_chars=['.','"',',','/','\'','&','%']

def get_holdings_table(symbol,yf_ticker):
	try:
		if yf_ticker and 'quoteType' in yf_ticker.info:
			qt=yf_ticker.info['quoteType']
		else:
			qt = 'UNKNOWN'
	except Exception as ex:
		sys.stderr.write("get_holdings_table: no info {0}:{1}\n".format(symbol,ex))
		qt='UNKNOWN'

	holdings_table=load_csv_holdings_table(symbol)	
	# bail - we loaded from csv
	if len(holdings_table['perc']) > 0:
		return(holdings_table)

	if qt == 'ETF':
		url = etf_holding_url.format(symbol)
		tab_pat = 'etf_holdings\\.formatted_data'    
		name_pat = 'title=\\\\"(.*?)\\\\'
		sym_pat=r'etf\\\/(.*?)\\'
		perc_col=3
		symbol_col=1
		name_col=0
	elif qt == 'MUTUALFUND':
		url = mf_holding_url.format(symbol)
		tab_pat = 'document\\.table_data'    
		name_pat = 'title=\\\\"(.*?)\\\\'
		sym_pat=r'quote\\\/(.*?)\\'
		perc_col=4
		symbol_col=0
		name_col=5
	elif qt == 'EQUITY': 
		holdings_table['symbol']=[symbol]
		holdings_table['name']=[yf_ticker.info['longName']]
		holdings_table['perc']=[100.]
		return(holdings_table)
	elif qt == 'MONEYMARKET': 
		holdings_table['symbol']=[symbol]
		holdings_table['name']=['CASH']
		holdings_table['perc']=[100.]
		return(holdings_table)
	else:
		holdings_table['symbol']=[symbol]
		holdings_table['name']=['UNKNOWN']
		holdings_table['perc']=[100.]
		return(holdings_table)

	try:
		r = get_page(url)
		tdat=''
		title_data=''
		title_col_toks = []
		title_pattern='document.table_title(.+?);'
		title_col_pattern='\{\s*"title":\s*"(.+?)".+?\}'
		m =re.search(title_pattern, r.text.replace('\n',' '))
		if m: 
			title_data=m.group(1)
			title_col_toks = re.findall(title_col_pattern,title_data)
			if 'Weight' in title_col_toks:
				perc_col = title_col_toks.index('Weight')
			if 'Symbol' in title_col_toks:
				symbol_col = title_col_toks.index('Symbol')
			if 'Company Name' in title_col_toks:
				name_col = title_col_toks.index('Company Name')
			# bond funds don't have symbols, just Securities
			if 'Security' in title_col_toks:
				symbol_col = title_col_toks.index('Security')
				name_col = title_col_toks.index('Security')
	
	#	print(title_data,title_col_toks)
		for l in r.text.split('\n'):
			if re.match(tab_pat,l.strip()):
				tdat=l
		perc=[]
		if len(tdat)>0:
			toks=tdat.split('[')[2:] # each table line
			for tline in toks:
				ptoks =  re.split(el_pat,tline)
				if 'NA' in ptoks[perc_col]:
					perc.append(0.)
				else:
					perc.append(float(ptoks[perc_col].replace('%','')))
			raw_symbols=[x.split(',')[symbol_col] for x in toks]
			symbols=[]
			for rs in raw_symbols:
				m = re.findall(sym_pat, rs )
				if m:
					symbols.append(m[0])
				else:
					symbols.append(rs.strip().replace('"',''))
			raw_names=[re.split(el_pat,x)[name_col] for x in toks]
			names=[]
			for rn in raw_names:
				m = re.findall(name_pat,rn)
				if m:
					names.append(m[0])
				else:
					names.append(rn.strip().replace('"',''))
	
		for n,s,p in zip(names,symbols,perc):
	#		print('\t'.join([s,"{0:.3f}".format(p),n]))
			holdings_table['symbol'].append(s)
			holdings_table['name'].append(n)
			holdings_table['perc'].append(p)

#	print(len(symbols),len(perc),len(names))
#	print(sum(perc))
	except Exception as ex:
		sys.stderr.write("Problem getting holdings {0}:{1}\n".format(symbol,ex))
	
	return(holdings_table)

def dump_raw(r,tag):
	with open(os.path.join(cache_dir,'{}.txt'.format(tag)),'w') as f:
		f.writelines(r.text)

def parse_raw_table(raw):
	rows = raw.split('<tr')
	table={}
	for row in rows:
		first_cols = re.findall('<th.*?>([\s\S]*?)</th',row.replace('\n',''))
		cols = re.findall('<td.*?>([\s\S]*?)</td',row.replace('\n',''))
		alt_hd = re.findall('<h2.*?>([\s\S]*?)</h2',row.replace('\n',''))
		if len(first_cols) > 0 and len(cols) > 0:
			key=first_cols[0].replace('%','').strip()
			if len(key)>0:
				table[key]=cols[0]
		elif len(cols)==2:
			key=cols[0].replace('%','').strip()
			if len(key)>0:
				table[key]=cols[1]
		elif len(alt_hd)==1:
			key=alt_hd[0].strip()
			if len(key)>0:
				table[key]=100.
	return(table)

info_pats = ['Expense Ratio','SEC Yield', 'Dividend \(Yield\)']

cat_LUT = {'MONEYMARKET':'Cash','EQUITY':'Stock'}

# loads optional csv sector table
def load_csv_sector_table(symbol,summary_table):
	sector_csv_file = os.path.join(cache_dir,symbol + "_sectors.csv")
	if os.path.isfile(sector_csv_file):
		csv = pd.read_csv(sector_csv_file,names=['sector','perc'])
		summary_table['SECT']={}
		for s,p in zip(csv['sector'],csv['perc']): 
			summary_table['SECT'][s]=float(p)
	return

# loads optional csv file
def load_csv_holdings_table(symbol):

	holdings_table={}
	holdings_table['symbol']=[]
	holdings_table['name']=[]
	holdings_table['perc']=[]

	# check for seperate csv file
	holdings_csv_file = os.path.join(cache_dir,symbol+"_holdings.csv")
	if os.path.isfile(holdings_csv_file):
		csv = pd.read_csv(holdings_csv_file)
		# need name and perc cols
		if 'name' in csv.columns and 'perc' in csv.columns:
			for n,p in zip(csv['name'],csv['perc']):
				for rc in replace_chars:
					n=n.replace(rc,'')
				holdings_table['name'].append(n.upper())
				holdings_table['perc'].append(float(p))
		else:
			sys.stderr.write("Missing name, perc cols in {0}\n".format(holdings_csv_file))
		# might have symbol column
		if 'symbol' in csv.columns:
			for s in csv['symbol']:
				holdings_table['symbol'].append(s)
	return(holdings_table)

def get_info_table(symbol, yf_ticker,summary_table=None):
	try:
		if yf_ticker and 'quoteType' in yf_ticker.info:
			qt=yf_ticker.info['quoteType']
		else:
			sys.stderr.write("get_info_table: problem getting info for {0}\n".format(symbol))
			qt="UNKNOWN"
	except Exception as ex:
		sys.stderr.write('get_info_table: no info for {0}: {1}\n'.format(symbol,ex))
		qt='UNKNOWN'

	# establish rational defaults as a template
	# for some things the tables need to be filled by hand
	if not summary_table:
		summary_table={}
	if qt in cat_LUT:
		summary_table['CAT']=cat_LUT[qt] 
	else:
		summary_table['CAT']=qt # first guess
	summary_table['ER']=0
	summary_table['SL']=0
	summary_table['DL']=0
	summary_table['DIV']=0
	summary_table['SECT']={}
	summary_table['QUOTE_TYPE']=qt

	exp_table={}
	fee_table={}
	sector_table={}
	stat_table={}
	cat_table={}
	if qt!='MONEYMARKET' and qt!='UNKNOWN': # bypass, use yfinance
		sector_pat = r'<section id="mf_sector">([\S\s]*?)</section'
		if qt == 'MUTUALFUND':
			exp_pat = r'<section id="mf_expenses">([\S\s]*?)</section'
			url = mf_url.format(symbol)
		elif qt == 'ETF':
			exp_pat = r'<section id="etf_expense_ratio">([\S\s]*?)</section'
			url = etf_url.format(symbol)
			sector_pat = r'<section id="etf_benchmark">([\S\s]*?)</section'
		elif qt == 'EQUITY':
			# should always fail for equities
			exp_pat = r'<section id="etf_expense_ratio">([\S\s]*?)</section'
			url = equity_url.format(symbol)
		fee_pat = r'<section id="mf_fees">([\S\s]*?)</section'
		stat_pat = r'<section id="mf_port_stat">([\S\s]*?)</section'
		cat_pat = r'<section id="mf_allocation">([\S\s]*?)</section'
		r = get_page(url)
		raw_exp = re.search(exp_pat, r.text)	
		raw_fee = re.search(fee_pat, r.text)	
		raw_sector = re.search(sector_pat,r.text)
		raw_stat = re.search(stat_pat,r.text)
		raw_cat = re.search(cat_pat,r.text)
		if raw_exp:
			exp_table = parse_raw_table(raw_exp[0])
#			print(exp_table)
		if raw_fee:
			fee_table = parse_raw_table(raw_fee[0])
#			print(fee_table)
		if raw_sector:
			sector_table = parse_raw_table(raw_sector[0])
#			print(sector_table)
		if raw_stat:
			stat_table = parse_raw_table(raw_stat[0])
#			print(stat_table)
		if raw_cat:
			cat_table = parse_raw_table(raw_cat[0])
#			print(cat_table)
		if len(stat_table) > 0:
			summary_table['STATS']=stat_table
		if len(cat_table) > 0:
			summary_table['CATEGORIES']=cat_table
		cat_pat='<a.+?Categories.+?>.+?<a.+?>(.+?)</a'
		m = re.search(cat_pat, r.text)
		if m:
#			print("Category: " + m.group(1))
			summary_table['CAT']=m.group(1)
		# use cat table instead
		if len(cat_table) > 0:
			# just take the max 
			cat_max=0
			for k in cat_table:
				try:
					cv = float(cat_table[k].replace('%',''))
				except ValueError as ve:
					cv = 0
				if cv > cat_max:
					cat_max=cv
					summary_table['CAT']=k	
		dump_raw(r,symbol)
	# Expense Ratio
	if 'Expense Ratio' in exp_table:
		summary_table['ER']=float(exp_table['Expense Ratio'].replace('%',''))
	# Front load
	if 'Max Sales Load' in fee_table and fee_table['Max Sales Load']!='NA':
		summary_table['SL']=float(fee_table['Max Sales Load'])
	# Back load
	if 'Max Deferred Load' in fee_table and fee_table['Max Deferred Load']!='NA':
		summary_table['DL']=float(fee_table['Max Deferred Load'])
	# Dividends or Yield
	if qt == 'EQUITY' or qt == 'MUTUALFUND' or qt =='MONEYMARKET':
		if 'yield' in yf_ticker.info:
			summary_table['DIV']=100*yf_ticker.info['yield']
	if qt =='ETF':
		# gaahh, another format!
		if 'Dividend (Yield)' in exp_table:
			m = re.search('\((.+?)%\)',exp_table['Dividend (Yield)'])
			if m:
				summary_table['DIV']=float(m.group(1))

	# Sector table	
	# look for associated file first
	load_csv_sector_table(symbol,summary_table)
	if len(summary_table['SECT'])==0: # look somewhere else
		if qt!='ETF':
			if len(sector_table) > 0:
				for s in sector_table:
					val=0
					if sector_table[s]!='NA':
						val=float(sector_table[s])
					summary_table['SECT'][s]=val
		if qt == 'EQUITY':
			summary_table['SECT'][yf_ticker.info['sector']]=100.
		if qt == 'MONEYMARKET':
			summary_table['SECT']['CASH']=100.
		if qt == 'UNKNOWN':
			summary_table['SECT']['UNKNOWN']=100.
		if qt == 'ETF':
			# have to look somewhere else for ETF index breakdowns
			r = get_page(etf_sector_url.format(symbol))
			etf_sector_pat='data-chart-series=(.+?)data-title=.Sector.*?Breakdown'
			m = re.search(etf_sector_pat, r.text)
			if m:
				sec_data='\{"name":(.*?),"data":\[(.+?)\]\}'
				sd = re.findall(sec_data,m.group(1))
				for s in sd:
					summary_table['SECT'][s[0].replace('"','')]=float(s[1])
			dump_raw(r, symbol+"_ETF")

	return(summary_table)


def update_price_table(symbol,prc,prices):
	# update price table
	if prices!=None:
		if not symbol in prices:
			prices[symbol]={}
		prices[symbol][prc.date]=prc
	return

def quote(symbol,tk=None,prices=None,quote_date=None,quiet=True):
	''' Arguments: string ticker symbol, optionally a yfinance ticker
		for this symbol (might already have one, so saves time)
		optional prices dict based on price directives in loaded ledger
		optional date for quote (otherwise use today)
		Returns Beancount Price named tuple
	'''
	# if not supplied, quote for today
	if not quote_date:
		quote_date = dt.date(dt.now())
	elif type(quote_date)==str:
		quote_date=dt.date(dt.fromisoformat(quote_date))
	# try the supplied price table
	if prices and symbol in prices:
		# get closest date 
		ta=np.array([x for x in prices[symbol].keys()])
		tidx=np.argmin(np.abs(quote_date-ta))
		ctd = cache_timeout_days
		prc = prices[symbol][ta[tidx]]
		if 'cache_timeout_days' in prc.meta:
			ctd = int(prc.meta['cache_timeout_days'])
		if ctd <=0 and len(ta) > 1: # interpolate if possible
			tat=np.array([dt(x.year,x.month,x.day).timestamp() for x in prices[symbol].keys()])
			tv = np.array([float(prices[symbol][k].amount[0]) for k in ta])
			qdt = dt(quote_date.year,quote_date.month,quote_date.day).timestamp()
			interp_v = np.interp(qdt, tat, tv)
			prc=prc._replace(amount=prc.amount._replace(number=round(Decimal(interp_v),5)))
			if not quiet:
				sys.stderr.write(
					"{0} using interpolated ledger price {1} from {2}\n".format(
					symbol,
					prc.amount,
					quote_date.isoformat(),
					)
				)	
			# note: don't update price table with interpolated values
			return(prc) 

		# if ctd < 0, just use whatever is closest no matter the timedelta
		# otherwise, use the quote within the timeout
		if ctd <= 0 or np.abs(ta[tidx] - quote_date) < timedelta(ctd):
			if not quiet:
				sys.stderr.write(
					"{0} using ledger price {1} from {2}\n".format(
					symbol,
					prc.amount,
					prc.date.isoformat(),
					)
				)	
			return(prc) # last quote price
	
	# Not in price table, or expired timeout
	# try Yahoo Finance for history
	start_date=(quote_date-timedelta(days=cache_timeout_days)).isoformat()
	end_date=(quote_date+timedelta(days=cache_timeout_days)).isoformat()
	if dt.fromisoformat(end_date) > dt.today():
		end_date=dt.date(dt.now()).isoformat()
	qv=Decimal('0.00')
	qd=quote_date
	qc='UNKNOWN'
	try:
		if not tk:
			tk = yf.ticker.Ticker(symbol)
		qtype='UNKNOWN'
		if 'quoteType' in tk.info:
			qtype=tk.info['quoteType']
		if qtype=='MONEYMARKET':
			qv = Decimal('1.00000')
			qc = tk.info['currency']
		if qtype !='MONEYMARKET' and qtype !='UNKNOWN':
			df=tk.history(start=start_date,end=end_date,auto_adjust=False)
			if len(df) > 0 and 'Close' in df.columns:
				ta=np.array([dt.date(x) for x in df.index])
				tidx=np.argmin(np.abs(ta-quote_date))
				if np.abs(ta[tidx]-quote_date) < timedelta(cache_timeout_days):
					qv = round(Decimal(df['Close'][tidx]),5)
					qc = tk.info['currency']
					qd = dt.date(df.index[tidx])
				# might as well add all the prices we just got
				for close_date, close_val in zip(df.index,df['Close']):
					prc = Price({},date=dt.date(close_date),currency=symbol,amount=Amount(round(Decimal(close_val),5),qc))
					update_price_table(symbol,prc,prices)
			else:
				sys.stderr.write("Unable to obtain quote for {}\n".format(symbol))
	except Exception as err:
		sys.stderr.write("Error getting quote {0}: {1}\n".format(symbol,err))

	prc = Price({},date=qd,currency=symbol,amount=Amount(qv,qc))
	if qc!='UNKNOWN':
		update_price_table(symbol,prc,prices)
		
	return(prc)

def check_cache(symbol):
	yaml_file=os.path.join(cache_dir,symbol+'.yaml')
	info_table={}
	if os.path.isfile(yaml_file):
		with open(yaml_file,'r') as f:
			info_table=yaml.safe_load(f)
		# check for csv files here
		holdings_table=load_csv_holdings_table(symbol)
		if len(holdings_table['perc']) > 0:
			info_table['HOLDINGS']=holdings_table
		sector_table = load_csv_sector_table(symbol,info_table)

	# if yaml file is recent, just return from cache
	if 'QUOTE_DATE' in info_table:
		tzi=tz('US/Pacific')
		right_now=tzi.localize(dt.now())
		last_quote_time=tzi.localize(dt.fromisoformat(info_table['QUOTE_DATE']))
		if right_now-timedelta(days=cache_timeout_days) < last_quote_time:
			sys.stderr.write("Returning yaml cache info for {}...\n".format(symbol))
		else:
			sys.stderr.write("Yaml cache out of date for {}...\n".format(symbol))
	return(info_table)

# call this function on each symbol in portfolio
def get_all(symbol,clobber=True,prices=None,quote_date=None):

	# always make sure cache dir exists
	if not os.path.isdir(cache_dir):
		os.makedirs(cache_dir)

	cache_info = False
	info_table = check_cache(symbol)
	if len(info_table) > 0:
		cache_info=True

	# always try to get a fresh quote - don't use yaml cache value
	tk = yf.ticker.Ticker(symbol)
	latest_quote=quote(symbol,tk,prices=prices,quote_date=quote_date)
	info_table['QUOTE_DATE']=latest_quote.date.isoformat()
	info_table['QUOTE']=float(latest_quote.amount[0])
	info_table['QUOTE_CURRENCY']=latest_quote.amount[1]
	if cache_info:
		return(info_table)

	# if we got here, have to reload information
	sys.stderr.write("Gathering info for {}...\n".format(symbol))
	try:
		info_table = get_info_table(symbol, tk, info_table)
		info_table['HOLDINGS']=get_holdings_table(symbol, tk)
		info_table['YF_TABLES']=get_yahoo(symbol)
	except Exception as ex:
		sys.stderr.write("get_all: Can't get info on {0}:{1}\n".format(symbol,ex))

	yaml_path = os.path.join(cache_dir,symbol+".yaml")
	if not os.path.isfile(yaml_path) or clobber:
		backup_file(yaml_path)
		with open(yaml_path,'w') as f:
			yaml.dump(info_table, f)	
		
	return(info_table)
