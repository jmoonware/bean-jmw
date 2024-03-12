# Shared routines for importers
# Importers fill out a standard named tuple transaction which
# are turned into Beancount Transactions here

from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad,NoneType
from beancount.core.number import MISSING
from collections import namedtuple

import os,sys, re
from datetime import datetime as dt

uni_row_fields = ['date', 'account', 'action', 'symbol', 'security_description', 'type', 'quantity', 'price', 'commission', 'fees', 'amount', 'settlement_date','payee','narration','category','memo']
  
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

# remove single quotes if present...
def unquote(s):
	unqs=s
	if s and len(s) > 0 and s[0]=="'" and s[-1]=="'":
		unqs = s[1:-1]
	return unqs


default_open_date='2000-01-01'

def extract(file, importer):
	"""Universal extract transactions from a file.
    Args:
      file: A cache.FileMemo instance.
      importer: an importer instance with the following member functions: 
		create_table
        map_universal_table
    Returns:
      A list of new, imported directives 
      extracted from the file.
	"""
	# call supplied importers table creator
	import_table = importer.create_table(file.name)

	# map table lines (or entries) onto UniRows
	uentries = importer.map_universal_table(import_table)

	# now fill out the Beancount Transactions from universal
	entries = get_transactions(uentries)
	
	# add whatever opens we need
	open_entries = create_opens(importer.account_name, importer.account_currency)

	return(open_entries + entries)

def create_opens(account_name,account_currency):
	# add open directives; some may be removed in dedup
	open_date=dt.date(dt.fromisoformat(default_open_date))
	open_entries=[Open({'lineno':0,'filename':account_name},open_date,a,["USD",c],Booking("FIFO")) for a,c in account_currency.items()]
	return(open_entries)

def fix_rounding(rec,acct):
	""" Kludge to fix reported rounding errors
		TODO: make more Beancounty
	"""
	meta=new_metadata(acct, 0)
	amt=Decimal(0)
	if rec.total!=0:
		amt=rec.total+Decimal('0.00')
	qty=Decimal(0.000000001)
	if rec.units > 0:
		qty=rec.units
	prc=Decimal(0)
	if rec.unit_price>0:
		prc=rec.unit_price
		tprc=abs(amt/qty)
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

def get_transactions(transactions,account_name,default_payee):
	''' Arguments: list of universal transaction named tuples
		Return: list of beancount entries
	'''
	entries=[]
	for unir in transactions:
		meta=new_metadata(account_name, 0)
		trn_date = get_trn_date(unir)
		if not trn_date:
			sys.stderr.write("Unknown date for transaction {0}\n".format(unir))
			continue
		tn=Transaction(
			meta=meta,
			date=dt.date(trn_date),
			flag="*",
			payee=default_payee,
			narration=unir.narration,
			tags=EMPTY_SET,
			links=EMPTY_SET,
			postings=generate_investment_postings(unir,account_name,currency),
		)
		entries.append(tn)

	return(entries)

def generate_investment_postings(uni,account_name,currency):
	postings=[]
	trn_date=dt.date(get_trn_date(uni))

	# set defaults for two generic postings (p0, p1)
	sec_currency=currency # default to this
	sec_name="Cash"
	sec_account=sec_name
	# investment account with security name
	if uni.symbol and uni.symbol!=currency:
		sec_currency=uni.symbol
		sec_account=uni.symbol
	acct = ":".join([account_name, sec_account])
	qty = Decimal('0')
	if uni.units:
		qty = uni.units
	elif uni.amount:
		qty = uni.amount
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
		acct = ":".join([account_name, sec_account])
		meta, amt, qty, prc = fix_rounding(uni,acct)
		postings[0]=p0._replace(
			account = acct,
			units=Amount(qty,sec_currency),
			price = Amount(prc,currency),
			cost = Cost(prc,currency,trn_date,""),
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
		if len(uni.commission)>0:
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
		total_cost=commission
		if uni.total!=0:
			total_cost=uni.total +commission
		prc=Decimal(0)
		if uni.unit_price>0:
			prc=uni.unit_price
		postings[0]=p0._replace(
			account = ":".join([account_name, sec_account]),
			units=Amount(uni.units,sec_currency),
			# let Beancount booking rules take care of cost
			cost=Cost(None,None,None,None),
			price = Amount(prc,currency),
		)
		postings[1]=p1._replace(
			account = account_name + ":Cash",
			units = Amount(total_cost-commission,currency)
		)
		# interpolated posting
		# Open account here as we need the Open to have no 
		# currency explicity defined to pass validation
		# the __residual__ is needed to pass validation
		# Rem to add __residual__ to EntryPrinter.META_IGNORE
		interp_acct = ":".join([self.account_name.replace('Assets','Income'),sec_account,"Gains"])
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
			units=Amount(-Decimal(uni.total),currency)
		)
		postings[1]=p1._replace(
			account = account_name + ":Cash",
			units = Amount(Decimal(uni.total),currency)
		)
	elif uni.action in ['ReinvDiv']:
		meta, amt, qty, prc = fix_rounding(uni,acct)
		postings[0]=p0._replace(
			account = ":".join([account_name.replace('Assets','Income'),sec_account,"Div"]),
			units=Amount(amt,currency)
		)
		postings[1]=p1._replace(
			account = ':'.join([account_name,sec_account]),
			units = Amount(qty,sec_currency),
			price = Amount(prc,currency),
			cost = Cost(prc,currency,trn_date,""),
			meta = meta, 
		)
	elif uni.action in ['IntInc','MiscInc']:
		if hasattr(uni, 'amount'):
			amt=uni.amount
		elif hasattr(uni, 'total'):
			amt=uni.total
		else:
			sys.stderr.write("Unknown amt in {0}\n".format(uni))
			amt=Decimal('0')
		postings[0]=p0._replace(
			account = ":".join([account_name.replace('Assets','Income'),uni.action]),
			units=Amount(-amt,currency)
		)
		postings[1]=p1._replace(
			account = account_name + ":Cash",
			units = Amount(amt,currency)
		)
	elif uni.action in ['MiscExp']:
		if hasattr(uni, 'amount'):
			amt=uni.amount
		elif hasattr(uni, 'total'):
			amt=uni.total
		else:
			sys.stderr.write("Unknown amt in {0}\n".format(uni))
			amt=Decimal('0')
		postings[0]=p0._replace(
			account = ":".join([account_name.replace('Assets','Expenses'),uni.action]),
			units=Amount(-amt,currency)
		)
		postings[1]=p1._replace(
			account = account_name + ":Cash",
			units = Amount(amt,currency)
		)
	elif uni.action in ['XIin','XOut']: 
		postings[0]=p0._replace(
			account = ":".join([account_name,"Cash"]),
			units=Amount(uni.total,currency)
		)
		postings[1]=p1._replace(
			account = ":".join([account_name, "Transfer"]),
			units=Amount(-uni.total,sec_currency),
		)
	# Merger just removes or adds shares at 0 cost - basis 
	# needs to be entered manually (maybe a way to get this?)
	elif uni.action == 'Merger':
		postings[0]=p0._replace(
			account = ":".join([account_name,sec_currency]),
			units=Amount(uni.units,sec_currency),
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
			units=Amount(-Decimal(uni.quantity),sec_currency),
		)
		postings[1]=p1._replace(
			account = account_name + ":FIXME",
			units = Amount(amt,currency)
		)
	# case of cash OfxTransaction
	# Single-leg this for non-investment accounts
	elif uni.action in ['Debit','Credit','Other']:
		if hasattr(uni, 'amount'):
			amt=uni.amount
		elif hasattr(uni, 'total'):
			amt=uni.total
		else:
			sys.stderr.write("Unknown transaction amt in {0}\n".format(uni))
			amt=Decimal('0')
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
		sys.stderr.write("Unknown investment action {0}\n".format(uni.action))

	return(postings)

def has_chrs(s):
	return(s!=None and len(s.strip())>0)
