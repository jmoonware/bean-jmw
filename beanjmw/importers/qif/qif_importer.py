# custom importer to load QIF files exported from Quicken
# use the custom qifparse package in https://github.com/jmoonware/qifparse

from beancount.ingest.importer import ImporterProtocol
from beancount.core.data import Transaction,Posting,Amount,new_metadata,EMPTY_SET,Cost,Decimal,Open,Booking,Pad, NoneType
from beancount.core.number import MISSING

import beanjmw.importers.importer_shared as impshare

import os,sys

from qifparse.parser import QifParser
from qifparse.qif import Transaction as QifTransaction
from qifparse.qif import Investment as QifInvestment

from datetime import datetime as dt

# remove these chars as Beancount accounts can't have them
quicken_category_remove=[' ','\'','&','-','+','.']

# all possible actions for qif investments
qif_investment_actions=[
'Buy', 'BuyX', 'Sell',  'SellX', 'CGLong', 'CGLongX', 'CGMid', 'CGMidX', 'CGShort', 'CGShortX', 'Div', 'DivX', 'IntInc', 'IntIncX', 'ReinvDiv', 'ReinvInt', 'ReinvLg', 'ReinvMd', 'ReinvSh', 'Reprice', 'XIn', 'XOut', 'MiscExp', 'MiscExpX', 'MiscInc', 'MiscIncX', 'MargInt', 'MargIntX', 'RtrnCap', 'RtrnCapX', 'StkSplit', 'ShrsOut', 'ShrsIn', 'Cash',
]

# map to universal
qif_action_map={
'Buy':'Buy', 'BuyX':'Buy', 'Sell':'Sell',  'SellX':'Sell', 
'CGLong':'CGLong', 'CGLongX':'CGLong', 
'CGMid':'CGShort', 'CGMidX':'CGShort', 
'CGShort':'CGShort', 'CGShortX':'CGShort', 
'Div':'Div', 'DivX':'Div', 'IntInc':'IntInc', 
'IntIncX':'IntInc', 'ReinvDiv':'ReinvDiv', 
'ReinvInt':'ReinvDiv', 'ReinvLg':'ReinvDiv', 
'ReinvMd':'ReinvDiv', 'ReinvSh':'ReinvDiv', 
'Reprice':'Reprice', 'XIn':'Xin', 'XOut':'Xout', 
'MiscExp':'MiscExp', 'MiscExpX':'MiscExp', 
'MiscInc':'MiscInc', 'MiscIncX':'MiscInc', 
'MargInt':'MiscInc', 'MargIntX':'MiscInc', 
'RtrnCap':'Other', 'RtrnCapX':'Other', 
'StkSplit':'StkSplit', 'ShrsOut':'ShrsOut', 
'ShrsIn':'ShrsIn', 'Cash':'Cash',
}

map_reinv = {
	'ReinvDiv':'Div',
	'ReinvLg':'CGLong',
	'ReinvMd':'CGShort',
	'ReinvSh':'CGShort',
} 

default_open_date='2000-01-01'

class Importer(ImporterProtocol):
	def __init__(self,account_name,currency='USD',account_number=''):
		self.account_name=account_name
		self.cash_acct = ''
		self.currency=currency
		# TODO: use account number
		self.account_number=account_number
		self.account_currency={} # added as discovered
		self.default_payee="QIF Transaction"
		super().__init__()

	def identify(self, file):
		"""Return true if this importer matches the given file.
			Args:
				file: A cache.FileMemo instance.
			Returns:
				A boolean, true if this importer can handle this file.
		"""
		if os.path.splitext(file.name)[1].upper()=='.QIF':
			head_lines=file.head().split('\n')
			found=False
			for ln in range(len(head_lines)):
				if '!Account' in head_lines[ln]:
					for l in head_lines[ln+1:]:
						if self.account_name in l:
							found=True
							break
			return found
		else:
			 return False

	def map_to_unirow(self,urd,payee_str,memo_str,num_str,category):
		if num_str:
			num_str=num_str.strip()
		else:
			num_str=""
		if category:
			# remove invalid chars, capitalize
			clean_category=category
			for c in quicken_category_remove:
				clean_category= clean_category.replace(c,"")
			cat_toks=clean_category.split(":")
			cap_cats=[]
			for ct in cat_toks: # Capitalize First LetterInWords
				cap_cats.append(ct[0].upper()+ct[1:]) 
			urd['category']=":".join(cap_cats)
		if len(num_str) > 0:
			num_str=" "+num_str
		check_str=""
		if payee_str:
			payee_str=payee_str.strip().replace('/','.')
			if "CHECK" in payee_str.upper():
				check_str="Check"
		else:
			payee_str=""
		n_toks=[payee_str,memo_str,check_str+num_str]
		 # truly blank
		if len(''.join(n_toks))==0 and not urd['category']:
			n_toks[0]='EMPTY' # for assigning later
		narration_str=" / ".join(n_toks)
		
		# update dict with values
		urd['payee']=payee_str
		urd['narration']=narration_str

		return


	def is_split_check(self,qt):
		ret=False
		if qt.payee and "CHECK" in qt.payee.upper():
			if hasattr(qt,"splits") and qt.splits and len(qt.splits)>0:
				ret=True
		return(ret)

	def clean_str(self,s):
		ret=""
		if s:
			ret=s.strip().replace('/','.')
		return(ret)
		
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
		qif=None
		with open(file.name,'r') as f:
			qif=QifParser(dayfirst=False).parse(f)	
		if not qif:
			sys.stderr.write("Unable to open or parse {0}".format(file.name))
			return(entries)
		self.security_list=qif.get_securities() # may be in qif export
		uentries = self.map_universal_table(qif.get_transactions(True)[0])
		entries = impshare.get_transactions(uentries, self.account_name, self.default_payee, self.currency, self.account_currency,self.cash_acct) 
		return(entries)

	def map_universal_table(self,transactions):
		uentries = []
		for qt in transactions: # qif transactions
			urd = impshare.UniRow()._asdict()
			if type(qt)==QifTransaction:
				# Special case: Used to record split checks for paying
				# credit cards - turn each split into a transaction
				# and mangle check number so payee isn't assigned from file
				if self.is_split_check(qt):
					for st in qt.splits:
						urd = impshare.UniRow()._asdict()
						payee_str="SPLIT "
						if st.memo:
							payee_str = payee_str + st.memo
						if st.category:
							payee_str = payee_str + " " + st.category
						self.map_to_unirow(urd, payee_str, self.clean_str(st.memo), "S"+self.clean_str(qt.num), st.category)
						urd['amount']=st.amount
						urd['action']='Debit'
						urd['date']=dt.date(qt.date)
						impshare.decimalify(urd)
						uentries.append(impshare.UniRow(**urd))
				else:
					self.map_to_unirow(urd, qt.payee, self.clean_str(qt.memo), qt.num, qt.category)
					urd['amount']=qt.amount
					urd['action']='Debit'
					urd['date']=dt.date(qt.date)
					impshare.decimalify(urd)
					uentries.append(impshare.UniRow(**urd))
			elif type(qt)==QifInvestment:
				# a single Investment record indicates a brokerage
				self.cash_acct = ':Cash'
				act_str=self.clean_str(qt.action)
				n_toks=[self.clean_str(qt.memo),act_str]
				 # truly blank
				if len(''.join(n_toks))==0 and (urd['category']==None or len(urd['category']==0)):
					n_toks[0]='EMPTY' # for assigning later
				urd['payee']="Investment from QIF"
				urd['memo']=self.clean_str(qt.memo)
				urd['narration']=" / ".join(n_toks)
				urd['category']=qt.category
				urd['amount']=qt.amount
				urd['date']=dt.date(qt.date)
				if act_str in qif_action_map:
					urd['action']= qif_action_map[act_str]
				else:
					sys.stderr.write("QIF Unknown action {0}\n".format(act_str))
				urd['quantity']=qt.quantity
				urd['price']=qt.price
				urd['description']=qt.security
				symbol = [s.symbol for s in self.security_list if s.name and s.name == qt.security]
				if len(symbol) > 0 and symbol[0]:
					urd['symbol']=symbol[0]
				impshare.decimalify(urd)
				uentries.append(impshare.UniRow(**urd))

		return(uentries)	

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

