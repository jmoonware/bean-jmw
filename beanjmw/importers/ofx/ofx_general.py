# custom importer to load generic OFX/QFX format using ofxparse

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType, Balance
from beancount.core.number import MISSING
import beanjmw.importers.importer_shared as impsh

# ofxparse uses either Transactions or InvestmentTransactions
# Transactions are simple debit/credit
from ofxparse import OfxParser

import os,sys, re

from datetime import datetime as dt

# all possible actions for investments
# OfxTransaction has DEBIT/CREDIT/OTHER
# all others arre for OfxInvestmentTransaction
investment_actions={
'debit':'Debit', 
'credit':'Credit', 
'other':'Other', 
'buydebt':'Buy', 
'buymf':'Buy', 
'buyopt':'Buy', 
'buyother':'Buy', 
'buystock':'Buy', 
'income':'MiscInc',
'invexpense':'MiscExp',
'reinvest':'ReinvDiv',
'selldebt':'Sell',
'sellmf':'Sell',
'sellopt':'Sell',
'sellother':'Sell',
'sellstock':'Sell',
'transfer':'XOut',
'split':'StkSplit',
}

default_open_date='2000-01-01'

class Importer(ImporterProtocol):
	def __init__(self,account_name,currency='USD',account_number=''):
		self.account_name=account_name
		self.account_number=account_number
		if len(account_number) > 0:
			self.acct_tail=self.account_number[-4:]
		else:
			self.acct_tok=self.account_name.split(':')[-1]
			self.acct_tail=self.acct_tok[len(self.acct_tok)-4:] 
		self.currency=currency
		self.account_currency={} # set up on init
		self.security_ids={} # LUT from uniqueid provided in transaction
		self.default_payee = "OFX Investment"
		super().__init__()

	def identify(self, file):
		"""Return true if this importer matches the given file.
			Args:
				file: A cache.FileMemo instance.
			Returns:
				A boolean, true if this importer can handle this file.
		"""
		ext = os.path.splitext(file.name)[1].upper()
		found=False
		if ext=='.OFX' or ext=='.QFX':
			try:
				with open(file.name) as f:
					tofx = OfxParser.parse(f)
				for ofx_acct in tofx.accounts:
					if ofx_acct.account_id[-4:]==self.acct_tail:
						found=True
			except:
				sys.stderr.write("Exception reading ofx file {0}\n".format(file.name))
			return found
		else:
			 return False
		
	def extract(self, file, existing_entries=None):
		"""Extract transactions from a file.
        Args:
          file: A cache.FileMemo instance.
          existing_entries: An optional list of existing directives 
        Returns:
          A list of new, imported directives (usually mostly Transactions)
          extracted from the file.
		"""
		entries=[]
		balances=[]
		try:
			with open(file.name,'r') as f:
				ofx=OfxParser.parse(f)
		except:
			sys.stderr.write("Unable to open or parse {0}".format(file.name))
			return(entries)
		
		# set up securities list if needed
		if hasattr(ofx, 'security_list'):
			self.account_currency[":".join([self.account_name,'Cash'])]=[self.currency]
			for s in ofx.security_list:
				if s.ticker and len(s.ticker) > 0:
					ticker=s.ticker
				else: # no ticker - happens with some accounts
					ticker=s.name.replace(' ','')[:20].upper()
				self.account_currency[':'.join([self.account_name,ticker])]=['USD',ticker]
				self.security_ids[s.uniqueid]=ticker
		else: # open a cash account
			self.account_currency[self.account_name]=[self.currency]
		for ofx_acct in ofx.accounts:
			if ofx_acct.account_id[-4:]==self.acct_tail:
				unrs = self.map_universal_table(ofx_acct.statement.transactions)
				entries = impsh.get_transactions(unrs, self.account_name, self.default_payee, self.currency, self.account_currency)
#				entries = self.get_transactions(ofx_acct.statement.transactions)
				balances = self.get_balances(ofx_acct.statement)

		# add open directives; some may be removed in dedup
#		open_date=dt.date(dt.fromisoformat(default_open_date))
#		open_entries=[Open({'lineno':0,'filename':self.account_name},open_date,a,c,Booking("FIFO")) for a,c in self.account_currency.items()]	
#		return(open_entries + entries + balances)
		return(entries + balances)

	def get_balances(self,stmt):
		""" Creates balance statements from OFX balances or positions
			Args: stmt = OFX statement
		"""
		balances=[]
		if hasattr(stmt, 'balance'): # Credit/Debit account
			nbal = Balance(
				meta=new_metadata("OFXFile", 0),
				date = dt.date(stmt.balance_date),
				account = self.account_name,
				amount = Amount(stmt.balance,stmt.currency.upper()),
				tolerance = None,
				diff_amount = None,
			)
			balances.append(nbal)
		elif hasattr(stmt, 'positions'): # investment account
			for p in stmt.positions:
				nbal = Balance(
					meta=new_metadata("OFXFile", 0),
					date = dt.date(p.date),
					account = ":".join([self.account_name,self.security_ids[p.security]]),
					amount = Amount(p.units,self.security_ids[p.security]),
					tolerance = None,
					diff_amount = None,
				)
				balances.append(nbal)
		return(balances)


	def file_account(self, file):
		"""Return an account associated with the given file.
        Args:
          file: A cache.FileMemo instance.
        Returns:
          The name of the account that corresponds to this importer.
		"""
		return(self.account_name)

	def file_name(self, file):
		"""A filter that optionally renames a file before filing.

        This is used to make tidy filenames for filed/stored document files. If
        you don't implement this and return None, the same filename is used.
        Note that if you return a filename, a simple, RELATIVE filename must be
        returned, not an absolute filename.

        Args:
          file: A cache.FileMemo instance.
        Returns:
          The tidied up, new filename to store it as.
		"""
		init_name=os.path.split(file.name)[1]
#		ds=dt.date(dt.fromtimestamp(os.path.getmtime(file.name))).isoformat()
		return(init_name)

	def file_date(self, file):
		"""Attempt to obtain a date that corresponds to the given file.

        Args:
          file: A cache.FileMemo instance.
        Returns:
          A date object, if successful, or None if a date could not be extracted.
          (If no date is returned, the file creation time is used. This is the
          default.)
		"""
		return

	def get_trn_date(self,fr):
		trn_date=None
		if hasattr(fr,'date'):
			trn_date=fr.date
		elif hasattr(fr, 'tradeDate'):
			# FIXME: when use tradeDate vs. settleDate?
			trn_date=fr.tradeDate
		return(trn_date)

	def map_universal_table(self,transactions):
		unirows=[]
		# look at ofx record attributes rather than a cartesion table	
		for ofxr in transactions:
			urd = impsh.UniRow()._asdict()
			if hasattr(ofxr, 'security'):
				urd['symbol']=self.security_ids[ofxr.security]
			if hasattr(ofxr, 'unit_price'):
				urd['price']=ofxr.unit_price
			if hasattr(ofxr, 'units'):
				urd['quantity']=ofxr.units
			if hasattr(ofxr, 'total'):
				urd['total']=ofxr.total
			if hasattr(ofxr, 'fees'):
				urd['fees']=ofxr.fees
			if hasattr(ofxr, 'commission'):
				urd['commission']=ofxr.commission
			if hasattr(ofxr, 'amount'):
				urd['amount']=ofxr.amount	
			if hasattr(ofxr, 'payee'):
				urd['payee'] = ofxr.payee
				narration_str=" / ".join([ofxr.payee, ofxr.memo,ofxr.type])
			else:
				narration_str=" / ".join([ofxr.memo,ofxr.type])
			urd['narration']=narration_str
			# TODO: Do we need a copy of memo?
			urd['memo']=ofxr.memo
			urd['description']=ofxr.memo
			urd['type']=ofxr.type
			trn_date = self.get_trn_date(ofxr)
			if not trn_date:
				sys.stderr.write("Unknown date for transaction {0}\n".format(ofxr))
				continue
			urd['date']=dt.date(trn_date)
			# action map is simple for ofx
			if ofxr.type in investment_actions:
				urd['action']=investment_actions[ofxr.type]
			# KLUDGE to deal with Etrade...
			if "DIV" in ofxr.memo and ofxr.type=='income':
				urd['action']="Div"
			if "INTEREST" in ofxr.memo and ofxr.type=='other':
				urd['action']="IntInc"
			unirows.append(impsh.UniRow(**urd))

		return(unirows)

	def get_transactions(self,transactions):
		entries=[]
		for ofxr in transactions:
			meta=new_metadata(self.account_name, 0)
			if hasattr(ofxr, 'payee'):
				narration_str=" / ".join([ofxr.payee, ofxr.memo,ofxr.type])
			else:
				narration_str=" / ".join([ofxr.memo,ofxr.type])
			trn_date = self.get_trn_date(ofxr)
			if not trn_date:
				sys.stderr.write("Unknown date for transaction {0}\n".format(ofxr))
				continue
			tn=Transaction(
				meta=meta,
				date=dt.date(trn_date),
				flag="*",
				payee=self.default_payee,
				narration=narration_str,
				tags=EMPTY_SET,
				links=EMPTY_SET,
				postings=self.generate_investment_postings(ofxr),
			)
			entries.append(tn)

		return(entries)

	def generate_investment_postings(self,fr):
		postings=[]
		trn_date=dt.date(self.get_trn_date(fr))
		# try to find investment action
		# switch to use QIF format names
		# TODO: Re-use code in qif importer
		ofx_action=None
		if fr.type in investment_actions:
			ofx_action=investment_actions[fr.type]
		# KLUDGE to deal with Etrade...
		if "DIV" in fr.memo and fr.type=='income':
			ofx_action="Div"

		if "INTEREST" in fr.memo and fr.type=='other':
			ofx_action="IntInc"

		# unsure what we should do here so bail
		if not ofx_action:
			sys.stderr.write("Unknown inv action: {0} in {1}\n".format(fr.type,fr))
			return(postings)
	
		# set defaults for two generic postings (p0, p1)
		sec_currency=self.currency # default to this
		sec_name="Cash"
		sec_account=sec_name
		# investment account with security name
		if hasattr(fr, 'security'):
			if fr.security in self.security_ids:
				sec_name=self.security_ids[fr.security]
				sec_currency=sec_name
				sec_account=sec_name
		acct = ":".join([self.account_name, sec_account])
		qty = Decimal('0')
		if hasattr(fr, 'units'):
			qty = fr.units
		elif hasattr(fr,'amount'):
			qty = fr.amount
		postings.append(
			Posting(
				account = self.account_name,
				units=Amount(qty,sec_currency),
				cost=None,
				price=None,
				flag=None,
				meta={}
			)
		)
		postings.append(
			Posting(
				account = self.account_name + ":Cash",
				units=Amount(-qty,self.currency),
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
		if ofx_action in ['Buy','ShrsIn']:
			acct = ":".join([self.account_name, sec_account])
			meta, amt, qty, prc = self.fix_rounding(fr,acct)
			postings[0]=p0._replace(
				account = acct,
				units=Amount(qty,sec_currency),
				price = Amount(prc,self.currency),
				cost = Cost(prc,self.currency,trn_date,""),
				meta = meta,
			)
			aname='Cash'
			# shares in came from a share exchange somewhere else
			if ofx_action == 'ShrsIn': # KLUDGE
				aname = 'Transfer'
			postings[1]=p1._replace(
				account = ":".join([self.account_name,aname]),
				units = Amount(-abs(amt),self.currency)
			)
		elif ofx_action=='Sell': 
			commission=Decimal(0)
			if len(fr.commission)>0:
				commission=fr.commission
				postings.append(
					Posting(
						account = self.account_name.replace('Assets','Expenses') + ":Commission",
						units=Amount(commission,self.currency),
						cost=None,
						price=None,
						flag=None,
						meta={}
					)
				)
			total_cost=commission
			if fr.total!=0:
				total_cost=fr.total +commission
			prc=Decimal(0)
			if fr.unit_price>0:
				prc=fr.unit_price
			postings[0]=p0._replace(
				account = ":".join([self.account_name, sec_account]),
				units=Amount(fr.units,sec_currency),
				# let Beancount FIFO booking rule take care
				cost=Cost(None,None,None,None),
				price = Amount(prc,self.currency),
			)
			postings[1]=p1._replace(
				account = self.account_name + ":Cash",
				units = Amount(total_cost-commission,self.currency)
			)
			# interpolated posting
			postings.append(
				Posting(
					account = self.account_name.replace('Assets','Income')+":Gains",
					units = NoneType(),
					cost = None,
					price = None,
					flag = None,
					meta={'__residual__':True}
				)
			)
		elif ofx_action in ['Div','CGShort','CGLong','CGMid']:
			postings[0]=p0._replace(
				account = ":".join([self.account_name.replace('Assets','Income'),sec_account,ofx_action]),
				units=Amount(-Decimal(fr.total),self.currency)
			)
			postings[1]=p1._replace(
				account = self.account_name + ":Cash",
				units = Amount(Decimal(fr.total),self.currency)
			)
		elif ofx_action in ['ReinvDiv']:
			meta, amt, qty, prc = self.fix_rounding(fr,acct)
			postings[0]=p0._replace(
				account = ":".join([self.account_name.replace('Assets','Income'),sec_account,"Div"]),
				units=Amount(amt,self.currency)
			)
			postings[1]=p1._replace(
				account = ':'.join([self.account_name,sec_account]),
				units = Amount(qty,sec_currency),
				price = Amount(prc,self.currency),
				cost = Cost(prc,self.currency,trn_date,""),
				meta = meta, 
			)
		elif ofx_action in ['IntInc','MiscInc']:
			if hasattr(fr, 'amount'):
				amt=fr.amount
			elif hasattr(fr, 'total'):
				amt=fr.total
			else:
				sys.stderr.write("Unknown amt in {0}\n".format(fr))
				amt=Decimal('0')
			postings[0]=p0._replace(
				account = ":".join([self.account_name.replace('Assets','Income'),ofx_action]),
				units=Amount(-amt,self.currency)
			)
			postings[1]=p1._replace(
				account = self.account_name + ":Cash",
				units = Amount(amt,self.currency)
			)
		elif ofx_action in ['MiscExp']:
			if hasattr(fr, 'amount'):
				amt=fr.amount
			elif hasattr(fr, 'total'):
				amt=fr.total
			else:
				sys.stderr.write("Unknown amt in {0}\n".format(fr))
				amt=Decimal('0')
			postings[0]=p0._replace(
				account = ":".join([self.account_name.replace('Assets','Expenses'),ofx_action]),
				units=Amount(-amt,self.currency)
			)
			postings[1]=p1._replace(
				account = self.account_name + ":Cash",
				units = Amount(amt,self.currency)
			)
		elif ofx_action in ['XIin','XOut']: 
			postings[0]=p0._replace(
				account = ":".join([self.account_name,"Cash"]),
				units=Amount(fr.total,self.currency)
			)
			postings[1]=p1._replace(
				account = ":".join([self.account_name, "Transfer"]),
				units=Amount(-fr.total,sec_currency),
			)
		# Merger just removes or adds shares at 0 cost - basis 
		# needs to be entered manually (maybe a way to get this?)
		elif ofx_action == 'Merger':
			postings[0]=p0._replace(
				account = ":".join([self.account_name,sec_currency]),
				units=Amount(fr.units,sec_currency),
				price = Amount(Decimal(0),self.currency),
			)
			meta=new_metadata(self.account_name, 0)
			meta["fixme"] = "Posting needs cost basis"
			postings[1]=p1._replace(
				account = ":".join([self.account_name, "Merger"]),
				units=Amount(Decimal(0),self.currency),
				price = Amount(Decimal(0),self.currency),
				meta = meta,
			)
		elif ofx_action=='StkSplit': 
			sys.stderr.write("StkSplit not implemented\n")
			pass
		# just remove shares - manually fix where they go later!
		# looks like sale for transfer between e.g. share classes 
		elif ofx_action=='ShrsOut': 
			# FIXME
			amt = Decimal('0') # Decimal(fr.amount) is empty!
			price = Decimal('0')
			postings[0]=p0._replace(
				account = ":".join([self.account_name, sec_account]),
				units=Amount(-Decimal(fr.quantity),sec_currency),
			)
			postings[1]=p1._replace(
				account = self.account_name + ":FIXME",
				units = Amount(amt,self.currency)
			)
		# case of cash OfxTransaction
		# Single-leg this for non-investment accounts
		elif ofx_action in ['Debit','Credit','Other']:
			if hasattr(fr, 'amount'):
				amt=fr.amount
			elif hasattr(fr, 'total'):
				amt=fr.total
			else:
				sys.stderr.write("Unknown transaction amt in {0}\n".format(fr))
				amt=Decimal('0')
			if ofx_action=='Other':
				aname=":".join([self.account_name,sec_account])
			else:
				aname=self.account_name
			postings[0]=p0._replace(
				account = aname,
				units=Amount(amt,self.currency),
			)
			if (fr.memo and len(fr.memo) > 0) or (hasattr(fr, 'payee') and fr.payee and len(fr.payee)>0): # assign later
				postings.remove(p1)
			else: # can't be assigned - have it come from Transfer
				postings[1]=p1._replace(
					account = self.account_name + ":Transfer",
					units = Amount(-amt,self.currency)
				)
		else:
			sys.stderr.write("Unknown investment action {0}\n".format(ofx_action))
	
		return(postings)

	def fix_rounding(self,rec,acct):
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
