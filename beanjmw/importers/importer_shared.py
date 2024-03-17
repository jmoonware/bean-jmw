# Shared routines for importers
# Importers fill out a standard named tuple transaction which
# are turned into Beancount Transactions here

from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad,NoneType
from beancount.core.number import MISSING
from collections import namedtuple

import os,sys, re
from datetime import datetime as dt

# Universal 'quantity' = Beancount 'units'
uni_row_fields = ['date', 'account', 'action', 'symbol', 'description', 'type', 'quantity', 'price', 'commission', 'fees', 'amount', 'settlement_date','payee','narration','category','memo','total']

# note setting of defaults  
UniRow = namedtuple('UniRow',uni_row_fields,defaults=[None for _ in range(len(uni_row_fields))])

# all possible actions for investments
# map_actions must return one of these and put in the action field of the UniRow to be parsed
investment_actions=[
'Buy', 
'Cash',
'Credit', 
'CGLong',
'CGShort',
'Div',
'Debit', 
'Merger',
'MiscInc',
'MiscExp',
'Other', 
'ReinvDiv',
'Sell',
'ShrsOut',
'ShrsIn',
'StkSplit',
'Xin',
'Xout',
]

# call this from each importer

def get_transactions(transactions,account_name,default_payee,currency,account_currency):
	''' Arguments: 
			transactions: list of universal transaction named tuples
			account_name: accout to extract to
			default_payee: String to add to payee
			currency: currency for account in account_name (usually USD)
			account_currency: dict of currencies to open by account name
		Return: list of beancount entries
	'''
	entries=[]
	for unir in transactions:
		if "IGNORE" in unir.description:
			continue
		meta=new_metadata(account_name, 0)
		tn=Transaction(
			meta=meta,
			date=unir.date, # should be datetime.date object
			flag="*",
			payee=default_payee,
			narration=unir.narration,
			tags=EMPTY_SET,
			links=EMPTY_SET,
			postings=generate_investment_postings(unir,account_name,currency,account_currency),
		)
		entries.append(tn)
	
	# add whatever opens we need
	open_entries = create_opens(account_name, account_currency)

	return(open_entries + entries)

# remove single quotes if present...
def unquote(s):
	unqs=s
	if s:
		unqs = s.strip()
	if unqs and len(unqs) > 0 and unqs[0]=="'" and unqs[-1]=="'":
		unqs = unqs[1:-1]
	return unqs


default_open_date='2000-01-01'

def create_opens(account_name,account_currency):
	# add open directives; some may be removed in dedup
	open_date=dt.date(dt.fromisoformat(default_open_date))
	open_entries=[Open({'lineno':0,'filename':account_name},open_date,a,c,Booking("FIFO")) for a,c in account_currency.items()]
	return(open_entries)

def decimalify(urd):
	# these names in the tuple should be converted to decimal 
	convert_names = ['amount','quantity','price','commission','fees','total']
	sfig = [2,4,2,2,2,2]
	for att,sf in zip(convert_names,sfig):
		if urd[att] and len(urd[att]) > 0:
			urd[att] = round(Decimal(urd[att]),sf)
	return

def build_narration(urd):
	# give us a narration for transaction
	t_info = [x for x in [urd['payee'],urd['memo'],urd['type'],urd['action'],urd['description']] if x]
	if len(t_info) > 0:
		urd['narration']=" / ".join(t_info)
	return

def fix_rounding(rec,acct):
	""" Kludge to fix reported rounding errors
		TODO: make more Beancounty
	"""
	meta=new_metadata(acct, 0)
	amt=Decimal(0)
	# use total if given, otherwise amount
	if rec.total and rec.total != 0:
		amt = rec.total + Decimal('0.00')
	elif rec.amount and rec.amount != 0:
		amt = rec.amount + Decimal('0.00')
	qty=Decimal(0)
	if rec.quantity != 0:
		qty=rec.quantity
	prc=Decimal(0)
	if rec.price and rec.price > 0 and qty != 0:
		prc=rec.price
		tprc=round(abs(amt/qty),6)
		if abs(qty*(tprc-prc)) > 0.0025: # exceeds tolerance
			meta["rounding"]="Price was {0}".format(prc)
			prc=tprc
	return meta, amt, qty, prc

def get_trn_date(uni):
	trn_date=None
	if hasattr(uni,'date'):
		trn_date=uni.date
	elif hasattr(uni, 'tradeDate'):
		# FIXME: when use tradeDate vs. settleDate?
		trn_date=uni.tradeDate
	return(trn_date)

def generate_investment_postings(uni,account_name,currency,account_currency):
	postings=[]

	# set defaults for two generic postings (p0, p1)
	sec_currency=currency # default to this
	sec_name="Cash"
	sec_account=sec_name
	# investment account with security name
	if uni.symbol and uni.symbol!=currency:
		sec_currency=uni.symbol
		sec_account=uni.symbol
	acct = ":".join([account_name, sec_account])
	account_currency[acct]=["USD",sec_currency]
	meta, amt, qty, prc = fix_rounding(uni,acct)
	if uni.action in ["Debit","Credit"]:
		acct_tail = ""
	else:
		acct_tail = ":Cash"
	postings.append(
		Posting(
			account = account_name,
			units=Amount(qty,sec_currency),
			cost=None,
			price=None,
			flag=None,
			meta={}
		)
	)
	postings.append(
		Posting(
			account = account_name + acct_tail,
			units=Amount(-qty,currency),
			cost=None,
			price=None,
			flag=None,
			meta={}
		)
	)
	# for convenience
	p0=postings[0]
	p1=postings[1] 

	# deal with each type of investment action:
	if uni.action in ['Buy','ShrsIn']:
		if sec_account=='Cash': # KLUDGE FOR FIDO 
			pcost = None
			pprice = None
		else:
			pcost = Cost(prc,currency,uni.date,"")
			pprice = Amount(prc,currency) 
		acct = ":".join([account_name, sec_account])
		postings[0]=p0._replace(
			account = acct,
			units=Amount(qty,sec_currency),
			price = pprice,
			cost = pcost,
			meta = meta,
		)
		aname='Cash'
		# shares in came from a share exchange somewhere else
		if uni.action == 'ShrsIn': # KLUDGE
			aname = 'Transfer'
		postings[1]=p1._replace(
			account = ":".join([account_name,aname]),
			units = Amount(-abs(amt),currency)
		)
	elif uni.action=='Sell': 
		commission=Decimal(0)
		if uni.commission:
			commission=uni.commission
			postings.append(
				Posting(
					account = account_name.replace('Assets','Expenses') + ":Commission",
					units=Amount(commission,currency),
					cost=None,
					price=None,
					flag=None,
					meta={}
				)
			)
		postings[0]=p0._replace(
			account = ":".join([account_name, sec_account]),
			units=Amount(qty,sec_currency),
			# let Beancount booking rules take care of cost
			cost=Cost(None,None,None,None),
			price = Amount(prc,currency),
		)
		postings[1]=p1._replace(
			account = account_name + ":Cash",
			units = Amount(amt-commission,currency)
		)
		# interpolated posting
		# Open account here as we need the Open to have no 
		# currency explicity defined to pass validation
		# the __residual__ is needed to pass validation
		# Rem to add __residual__ to EntryPrinter.META_IGNORE
		interp_acct = ":".join([account_name.replace('Assets','Income'),sec_account,"Gains"])
		account_currency[interp_acct]=None
		postings.append(
			Posting(
				account = interp_acct,
				units = NoneType(),
				cost = None,
				price = None,
				flag = None,
				meta={'__residual__':True},
			)
		)
	elif uni.action in ['Div','CGShort','CGLong','CGMid']:
		postings[0]=p0._replace(
			account = ":".join([account_name.replace('Assets','Income'),sec_account,uni.action]),
			units=Amount(-amt,currency)
		)
		postings[1]=p1._replace(
			account = account_name + ":Cash",
			units = Amount(amt,currency)
		)
	elif uni.action in ['ReinvDiv']:
		postings[0]=p0._replace(
			account = ":".join([account_name.replace('Assets','Income'),sec_account,"Div"]),
			units=Amount(amt,currency)
		)
		postings[1]=p1._replace(
			account = ':'.join([account_name,sec_account]),
			units = Amount(qty,sec_currency),
			price = Amount(prc,currency),
			cost = Cost(prc,currency,uni.date,""),
			meta = meta, 
		)
	elif uni.action in ['IntInc','MiscInc']:
		postings[0]=p0._replace(
			account = ":".join([account_name.replace('Assets','Income'),uni.action]),
			units=Amount(-amt,currency)
		)
		postings[1]=p1._replace(
			account = account_name + ":Cash",
			units = Amount(amt,currency)
		)
	elif uni.action in ['MiscExp']:
		postings[0]=p0._replace(
			account = ":".join([account_name.replace('Assets','Expenses'),uni.action]),
			units=Amount(-amt,currency)
		)
		postings[1]=p1._replace(
			account = account_name + ":Cash",
			units = Amount(amt,currency)
		)
	elif uni.action in ['Xin','Xout']: 
		postings[0]=p0._replace(
			account = ":".join([account_name,"Cash"]),
			units=Amount(amt,currency)
		)
		postings[1]=p1._replace(
			account = ":".join([account_name, "Transfer"]),
			units=Amount(-amt,sec_currency),
		)
	# Merger just removes or adds shares at 0 cost - basis 
	# needs to be entered manually (maybe a way to get this?)
	elif uni.action == 'Merger':
		postings[0]=p0._replace(
			account = ":".join([account_name,sec_currency]),
			units=Amount(qty,sec_currency),
			price = Amount(Decimal(0),currency),
		)
		meta=new_metadata(account_name, 0)
		meta["fixme"] = "Posting needs cost basis"
		postings[1]=p1._replace(
			account = ":".join([account_name, "Merger"]),
			units=Amount(Decimal(0),currency),
			price = Amount(Decimal(0),currency),
			meta = meta,
		)
	elif uni.action=='StkSplit': 
		sys.stderr.write("StkSplit not implemented\n")
		pass
	# just remove shares - manually fix where they go later!
	# looks like sale for transfer between e.g. share classes 
	elif uni.action=='ShrsOut': 
		# FIXME
		amt = Decimal('0') # Decimal(uni.amount) is empty!
		price = Decimal('0')
		postings[0]=p0._replace(
			account = ":".join([account_name, sec_account]),
			units=Amount(-qty,sec_currency),
		)
		postings[1]=p1._replace(
			account = account_name + ":FIXME",
			units = Amount(amt,currency)
		)
	# case of cash OfxTransaction
	# Single-leg this for non-investment accounts
	elif uni.action in ['Debit','Credit','Other']:
		postings[0]=p0._replace(
			account = ":".join([account_name, sec_account]),
			units=Amount(amt,currency),
		)
		# assign later if we can
		if has_chrs(uni.memo) or has_chrs(uni.payee) or has_chrs(uni.narration): 
			postings.remove(p1)
		else: # can't be assigned - have it come from Transfer
			postings[1]=p1._replace(
				account = account_name + ":Transfer",
				units = Amount(-amt,currency)
			)
	else:
		sys.stderr.write("Unknown investment action {0}\n".format(uni))

	return(postings)

def has_chrs(s):
	return(s!=None and len(s.strip())>0)