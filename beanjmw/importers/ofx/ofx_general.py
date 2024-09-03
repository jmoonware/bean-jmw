# custom importer to load generic OFX/QFX format using ofxparse

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType, Balance
from beancount.core.number import MISSING
import beanjmw.importers.importer_shared as impshare

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
'transfer':'Xout',
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
		self.cash_acct = ''
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
			# any account with a security_list is a brokerage (not a simple
			# debit/credit account (e.g. bank or credit card)
			self.cash_acct = ":Cash"
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
				entries = impshare.get_transactions(unrs, self.account_name, self.default_payee, self.currency, self.account_currency, self.cash_acct)
				balances = self.get_balances(ofx_acct.statement)

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
#					tolerance = None,
					tolerance = 0.001,
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
			urd = impshare.UniRow()._asdict()
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
			unirows.append(impshare.UniRow(**urd))

		return(unirows)

