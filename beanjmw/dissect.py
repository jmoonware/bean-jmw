import requests
import re
import yfinance as yf
import sys,os
from datetime import datetime as dt
from datetime import timedelta
from pytz import timezone as tz
import yaml
from bs4 import BeautifulSoup as BS

headers = {
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0"
}

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

def get_page(url):
	r=''
	with requests.Session() as req:
		req.headers.update(headers)
		r = req.get(url)
	return(r)

def get_yahoo(symbol):
	r = get_page(yf_holding_url.format(symbol))
	soup = BS(r.text,features='lxml')
	tabs = soup.find_all('div',{'class': 'Mb(25px)'}) 
	row_pat = '<span>([a-z /A-Z]+?)</span>'
	data_pat = '<span.*?>([0-9]+\.*[0-9]*%*|\s*N/A\s*)</span>'
	tables=[]
	titles=[]
	ret={}
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
	return(ret)			

# splits on double quote, comma, double quote with optional whitespace
el_pat = '" *, *"'

def get_holdings_table(symbol,yf_ticker):
	qt=yf_ticker.info['quoteType']
	holdings_table={}
	holdings_table['symbol']=[]
	holdings_table['name']=[]
	holdings_table['perc']=[]

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
		holdings_table['name']=yf_ticker.info['longName']
		holdings_table['perc']=[100.]
		return(holdings_table)
	elif qt == 'MONEYMARKET': 
		holdings_table['symbol']=[symbol]
		holdings_table['name']=['CASH']
		holdings_table['perc']=[100.]
		return(holdings_table)

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
	
	return(holdings_table)

def dump_raw(r,tag):
	with open('{}.txt'.format(tag),'w') as f:
		f.writelines(r.text)

def parse_raw_table(raw):
	rows = raw.split('<tr')
	table={}
	for row in rows:
		first_cols = re.findall('<th.*?>([\s\S]*?)</th',row.replace('\n',''))
		cols = re.findall('<td.*?>([\s\S]*?)</td',row.replace('\n',''))
		alt_hd = re.findall('<h2.*?>([\s\S]*?)</h2',row.replace('\n',''))
		if len(first_cols) > 0 and len(cols) > 0:
			table[first_cols[0].replace('%','').strip()]=cols[0]
		elif len(cols)==2:
			table[cols[0].replace('%','').strip()]=cols[1]
		elif len(alt_hd)==1:
			table[alt_hd[0].strip()]=100.
	return(table)

info_pats = ['Expense Ratio','SEC Yield', 'Dividend \(Yield\)']

def get_info_table(symbol, yf_ticker):
	summary_table={}
	qt=yf_ticker.info['quoteType']
	exp_table={}
	fee_table={}
	sector_table={}
	stat_table={}
	if qt!='MONEYMARKET': # bypass, use yfinance
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
		r = get_page(url)
		raw_exp = re.search(exp_pat, r.text)	
		raw_fee = re.search(fee_pat, r.text)	
		raw_sector = re.search(sector_pat,r.text)
		raw_stat = re.search(stat_pat,r.text)
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
		cat_pat='<a.+?Categories.+?>.+?<a.+?>(.+?)</a'
		m = re.search(cat_pat, r.text)
		if m:
#			print("Category: " + m.group(1))
			summary_table['CAT']=m.group(1)
		dump_raw(r,symbol)
	# Expense Ratio
	summary_table['ER']=0
	if 'Expense Ratio' in exp_table:
		summary_table['ER']=float(exp_table['Expense Ratio'].replace('%',''))
	# Front load
	summary_table['SL']=0
	if 'Max Sales Load' in fee_table and fee_table['Max Sales Load']!='NA':
		summary_table['SL']=float(fee_table['Max Sales Load'])
	# Back load
	summary_table['DL']=0
	if 'Max Deferred Load' in fee_table and fee_table['Max Deferred Load']!='NA':
		summary_table['DL']=float(fee_table['Max Deferred Load'])
	# Dividends or Yield
	summary_table['DIV']=0
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
	summary_table['SECT']={}
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

cache_timeout_days=3
def quote(symbol,tk):
	info_table = check_cache(symbol)
	if len(info_table) > 0 and 'QUOTE' in info_table and 'QUOTE_DATE' in info_table:
		return(info_table['QUOTE'],info_table['QUOTE_DATE'])

	# either expired cache or no info
	start_date=dt.date(dt.now()-timedelta(days=cache_timeout_days)).isoformat()
	if not tk:
		tk = yf.ticker.Ticker(symbol)
	df=tk.history(start=start_date)
	qt=0
	if len(df) > 0 and 'Close' in df.columns:
		qt = float(df['Close'][-1])
		qd = df.index[-1]
	return(qt,qd)

def check_cache(symbol):
	yaml_file=symbol+'.yaml'
	info_table={}
	if os.path.isfile(yaml_file):
		with open(yaml_file,'r') as f:
			info_table=yaml.safe_load(f)
	# if yaml file is recent, just return from cache
	if 'QUOTE_DATE' in info_table:
		last_quote_time=dt.fromisoformat(info_table['QUOTE_DATE'])
		tzi=tz('US/Pacific')
		right_now=tzi.localize(dt.now())
		if right_now-timedelta(days=cache_timeout_days) < last_quote_time:
			sys.stderr.write("Returning cache info for {}...\n".format(symbol))
	return(info_table)

# call this function on each symbol in portfolio
def get_all(symbol):

	info_table = check_cache(symbol)
	if len(info_table) > 0:
		return(info_table)

	# if we got here, have to reload information
	sys.stderr.write("Gathering info for {}...\n".format(symbol))
	tk = yf.ticker.Ticker(symbol)
	info_table = get_info_table(symbol, tk)
	latest_quote=quote(symbol,tk)
	info_table['QUOTE_DATE']=latest_quote[1].isoformat()
	info_table['QUOTE']=latest_quote[0]
	info_table['HOLDINGS']=get_holdings_table(symbol, tk)
	info_table['QUOTE_TYPE']=tk.info['quoteType']
	info_table['YF_TABLES']=get_yahoo(symbol)
	
	with open(symbol+".yaml",'w') as f:
		yaml.dump(info_table, f)	

	return(info_table)
