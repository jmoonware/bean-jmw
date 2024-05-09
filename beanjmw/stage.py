# Stage: Automates repetitive tasks for importing new entries into 
# separated beancount ledgers
#
# Add ledgersbyacct dict to acct.py

import argparse 
import shutil
import sys, os
import glob
import subprocess

from beancount.loader import load_file
from beancount.core.data import Transaction, Balance
from beancount.parser import printer

from datetime import datetime as dt

# TODO: make paths configurable
download_path = os.path.abspath(os.curdir)
yaml_path = os.path.join(download_path,"yaml")

sys.path.insert(0, download_path)
# add current path too, so importers work
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

ledger_path = ".."
staging_path = "../staging"

# relative to ledger_path
backup_folder = "backup"
bc_ext = ".bc" # beancount file extension

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

delete_suffix = ".deleteme"


ap = argparse.ArgumentParser()

ap.add_argument("-c","--check",required=False,help="Only run bean-check",default=False,action="store_true")
ap.add_argument("-e","--extract",required=False,help="Extract latest downloads, make release candidates, check",default=False,action="store_true")
ap.add_argument("--clean",required=False,help="Clean up yaml files",default=False,action="store_true")
ap.add_argument("-v","--verbose",required=False,help="Print all details",default=False,action="store_true")
ap.add_argument("--update",required=False,help="Backup orig ledger, move orig to delete file, move release candidate to orig",default=False,action="store_true")
ap.add_argument("--remove",required=False,help="Remove any undeleted safety files",default=False,action="store_true")
ap.add_argument("--test",required=False,help="Just print commands to be executed, but don't actually do anything",default=False,action="store_true")
ap.add_argument("--force",required=False,help="Force file update even if bean-check fails",default=False,action="store_true")
ap.add_argument("--split",required=False,help="Split this file into sub-ledgers using ledgersbyacct in accts.py",default='')
ap.add_argument("--last",required=False,help="Prints latest date in each ledger (useful to know when downloading new files",default=False,action='store_true')

clargs = ap.parse_args(sys.argv[1:])

try:
	from accts import ledgersbyacct
except ImportError as ie:
	sys.stderr.write(bcolors.FAIL + "Can't import ledgersbyacct from accts: {0}\n".format(ie) + bcolors.ENDC)
	sys.exit(1)

def is_nothing(fn):
	if os.path.isfile(fn):
		with open(fn) as f:
			lines=f.readlines()
	else:
		sys.stderr.write(bcolors.WARNING + 
			"Warning: didn't find {0}".format(fn) + 
			bcolors.ENDC+"\n")
		return(True)
	nothing = False
	for l in lines:
		if "Nothing to do" in l:
			nothing = True
	return(nothing)

def check(acct,rc_path):
	passed = False
	p = subprocess.Popen(["bean-check",rc_path],stderr=subprocess.PIPE)
	lines = p.stderr.readlines()
	if len(lines) > 0:
		full_rc_path = os.path.abspath(rc_path)
		nerr = len([x for x in lines if full_rc_path in x.decode('utf-8')])
		print(bcolors.FAIL+ "Errors for {0}, {1}: {2}".format(acct,rc_path,nerr) + bcolors.ENDC)
		if clargs.verbose:
			for x in lines:
				l = x.decode('utf-8').strip()
				if len(l) > 0:
					print(l)
	else:
		print(bcolors.OKGREEN+"Check passed for {0}, {1}".format(acct,rc_path)+bcolors.ENDC)
		passed=True
	return(passed)

def get_filenames(filebase, ext = bc_ext):
	epath = os.path.join(ledger_path,filebase + ext)
	new_path = os.path.join(staging_path,filebase + "_new" + ext)
	rc_path = os.path.join(staging_path, filebase + "_rc" + ext)
	bpath = os.path.join(backup_path(), filebase) + ext + ".bak"
	return epath, new_path, rc_path, bpath

def remove_marked():
	deletes = []
	for dpath in [ledger_path, staging_path, yaml_path]:
		deletes.extend(glob.glob(os.path.join(dpath,"*"+delete_suffix)))
	error = None
	if len(deletes) > 0:
		if clargs.remove:
			for delfile in deletes:
				try:
					if clargs.test:
						print("rm {0}".format(delfile))
					else:
						os.remove(delfile)
				except Exception as ex:
					sys.stderr.write("Problem deleting {0}: {1}\n".format(delfile,ex))
					error = str(ex)
		else:
			sys.stderr.write("Found some undeleted files {0}\n".format(deletes))
			sys.stderr.write("Remember to delete {0} files manually\n".format(delete_suffix))
	return(error)

def run_command(com):
	error = None
	if clargs.test:
		print(com)
	else:
		try:
			os.system(com)
		except Exception as ex:
			sys.stderr.write(bcolors.FAIL + "Error: run_command: {0}: {1}".format(com,str(ex))+bcolors.ENDC)
			error = str(ex)
	return(error)

def print_last():
	for acct in ledgersbyacct:
		epath, new_path, rc_path, bpath = get_filenames(ledgersbyacct[acct])
		if os.path.isfile(epath):
			com = "bean-query {0} 'select last(date)'".format(epath)
			sys.stderr.write(bcolors.OKBLUE + "Latest record for {0}:".format(epath) + bcolors.ENDC + "\n")
			run_command(com)
			sys.stderr.write('\n')

def extract_files():
	make_path(staging_path)
	for acct in ledgersbyacct:
		epath, new_path, rc_path, bpath = get_filenames(ledgersbyacct[acct])
		if os.path.isfile(epath):
			ecom = "python -m beanjmw.bci extract -e {0} -a {1} > {2}".format(epath,acct,new_path)
		else:
			ecom = "python -m beanjmw.bci extract -a {0} > {1}".format(acct,new_path)
		if not clargs.check:
			check_fatal_error(run_command(ecom))
			if is_nothing(new_path):
				print(bcolors.OKBLUE+"Nothing to do for {0}, {1}".format(acct,new_path)+bcolors.ENDC)
			#	ccom = "cat {0} > {1}".format(new_path,rc_path)
			#	check_fatal_error(run_command(ccom))
			else:
				if os.path.isfile(epath):
					ccom = "cat {0} {1} > {2}".format(epath,new_path,rc_path)
				else:
					ccom = "cat {0} > {1}".format(new_path,rc_path)
				check_fatal_error(run_command(ccom))
		# do a check of result
		if os.path.isfile(rc_path):
			passed = check(acct,rc_path)
	return

def make_path(bdir):
	error = None
	if not os.path.isdir(bdir):
		try:
			if clargs.test:
				print("mkdir {0}".format(bdir))
			else:
					os.makedirs(bdir)
		except Exception as ex:
			sys.stderr.write("Problem creating {0}: {1}\n".format(bdir,ex))
			error = str(ex)
	return(error)

def backup_path():
	return(os.path.join(ledger_path,backup_folder))

def versioned_filename(bpath):
	nbpath = str(bpath)
	if os.path.isfile(bpath):
		version=0
		nbpath = '.'.join([bpath,str(version)])
		while os.path.isfile(nbpath):
			version+=1
			nbpath = '.'.join([bpath,str(version)])
	return(nbpath)

def update_files():
	''' Backs up old ledger, writes new ledger from release candidate
	'''
	# don't proceed if this fails
	check_fatal_error(remove_marked())

	# Bool to set if something to update
	update_avalilable = [False]*len(ledgersbyacct)

	# make sure bean-check isn't giving errors
	for acct_idx, acct in enumerate(ledgersbyacct):
		epath, new_path, rc_path, bpath = get_filenames(ledgersbyacct[acct])
		if os.path.isfile(rc_path):
			if not check(acct,rc_path):
				sys.stderr.write(bcolors.WARNING + "Failed check {0}, {1}\n".format(acct,rc_path) + bcolors.ENDC)
				if not clargs.force:
					sys.stderr.write("Use --force to override\n")
				else:
					update_avalilable[acct_idx]=True
			else:
				update_avalilable[acct_idx]=True

	# now actually move the files around	
	ok_remove = True
	check_fatal_error(make_path(backup_path()))
	for do_update, acct in zip(update_avalilable, ledgersbyacct):
		if not do_update:
			sys.stderr.write(bcolors.OKBLUE + "Nothing to update for {0}\n".format(acct) + bcolors.ENDC)
			continue

		epath, new_path, rc_path, bpath = get_filenames(ledgersbyacct[acct])
	
		# we have an existing ledger - make a backup
		if os.path.isfile(epath):
			# make copy of original and verify
			nbpath = versioned_filename(bpath)
			if clone_file(epath,nbpath,True) == None: 
				# move existing ledger to a file marked for deletion
				if clone_file(epath, epath+delete_suffix)!=None:
					ok_remove = False
		# ledger should be moved or non-existant at this point
		# move release candidate to current active ledger file
		if not os.path.isfile(epath) and os.path.isfile(rc_path):
			if clone_file(rc_path,epath) != None:
				ok_remove=False
		else:
			if os.path.isfile(rc_path):
				sys.stderr.write("File exists - can't update {0}\n".format(epath))
				ok_remove = False
	# finally, clean up marked files
	if ok_remove:
		remove_marked()
	return

def clone_file(src,dst, copy = False):
	error = None
	if clargs.test:
		if copy:
			print("cp {0} {1}".format(src,dst))
		else:
			print("mv {0} {1}".format(src,dst))
	else:
		try:
			if copy:
				shutil.copy(src,dst)
				# verify the copy happened correctly!
				if not os.path.isfile(dst) or os.path.getsize(dst)!=os.path.getsize(src):
					raise FileNotFoundError("Problem with copy of {0} ==> {1}".format(src,dst))
			else:
				shutil.move(src,dst)
		except Exception as ex:
			sys.stderr.write(
				bcolors.FAIL + "Error cloning {0} to {1}: {2}\n".format(
				src,
				dst,
				ex) + bcolors.ENDC)
			error = str(ex)
	return(error)	

def check_fatal_error(msg):
	if msg!=None:
		sys.stderr.write(bcolors.FAIL + "Fatal Error: {0}\n".format(msg) + bcolors.ENDC)
		sys.stderr.write("Goodbye\n")
		sys.exit(1)
	return
	
def clean_yaml():
	''' sorts and simplifies yaml rules, removes unassigned yaml files
	'''
	check_fatal_error(make_path(backup_path()))
	# delete unassigned files
	deletions = glob.glob(os.path.join(yaml_path,"*_unassigned.yaml"))
	for dfile in deletions:
		check_fatal_error(clone_file(dfile,dfile+delete_suffix))
	# make backup of orig first
	backups = glob.glob(os.path.join(yaml_path,"*.yaml"))
	for src in backups:
		bpath = os.path.join(backup_path(),os.path.basename(src)+'.bak')
		dst = versioned_filename(bpath)
		check_fatal_error(clone_file(src,dst,copy=True))
	# now sort and simplify rules (ignore unassigned)
	yamls = [f for f in glob.glob(os.path.join(yaml_path, "*.yaml")) if not "unassigned" in f]
	for yfile in yamls:
		tdst = yfile + ".tmp"
		com = "python -m beanjmw.yaml_util -e {0} -sp > {1}".format(
					yfile,
					tdst,	
				)
		check_fatal_error(run_command(com))
		check_fatal_error(clone_file(yfile, yfile+delete_suffix))
		check_fatal_error(clone_file(tdst,yfile))
	# get rid of marked files
	check_fatal_error(remove_marked())

	return

def fix_transfer(e,acct,transfer_accts):
	''' Changes inter-account postings from acct into intermediate
		transfer account ("Assets:Transfer")
		Inserts split transactions if needed
	''' 
	splits = [] # new transactions to be added to another account ledger
	removes = [] # remove these possible duplicates 
	if type(e) == Transaction:
		# gather accounts used in this transaction
		paccts = [p.account for p in e.postings]
		mat = [acct in x for x in paccts]
		acct_idx = [i for i,x in enumerate(mat) if x]
		if len(acct_idx) > 0:
			tact_idx = []
			for tact in transfer_accts:
				mat = [tact in x for x in paccts]
				[tact_idx.append(i) for i,x in enumerate(mat) if x and not i in acct_idx]
			if len(tact_idx) > 0: # found some transfers
				for idx in tact_idx:
					nacct = "Assets:Transfer"
					# have to inject new balancing transaction
					# coming from transfer and going to the account
					# have to also remove the transaction that could appear
					# in more than one ledger split
					new_pos = []
					new_pos.append(e.postings[idx]._replace())
					new_pos.append(
						e.postings[idx]._replace(
							account=nacct,
							units = -e.postings[idx].units,
						)
					)
					removes.append(e._replace())
					splits.append(e._replace(postings=new_pos))
					e.postings[idx]=e.postings[idx]._replace(account=nacct)	

	return splits, removes

def fix_transfers(entries):
	""" Fixes postings between accounts in ledger_accounts 
		(i.e., turns the posting into a Transfer)
	"""
	new_split = []
	new_rm = []
	for acct in ledgersbyacct:
		for e in entries:
			spts, rms = fix_transfer(e, acct, list(ledgersbyacct.keys()))
			new_split.extend(spts)
			new_rm.extend(rms)

	return new_split, new_rm

def has_acct(acct,e):
	ret = False
	if type(e) == Transaction:
		paccts = [p.account for p in e.postings]
		mat = [acct in pa for pa in paccts]
		if sum(mat) > 0: # at least one was True
			ret = True
	elif type(e) == Balance:
		if acct in e.account:
			ret = True
	return(ret)

def remove_entry(e, entries):
	if e in entries:
		entries.remove(e)
	return

def split_ledger():
	""" Splits input ledger specified in command line into sub-ledgers
		Will create a common.bc file for common e.g. Open, Commodity, Price
		directives. Inserts transfer account transactions as needed
	"""
	all_entries, errors, config = load_file(clargs.split)
	if len(errors) > 0:
		check_fatal_error("Can't open or load {0}: {1}".format(clargs.split,errors))

	# now make the splits on the remediated entries
	common = []
	entriesbyacct = {}
	for acct in ledgersbyacct:
		entriesbyacct[acct]=[]
	for e in all_entries:
		is_common=True
		for acct in ledgersbyacct:
			# note: transactions going to multiple ledger accounts will
			# end up with a copy in each ledger entry list (will remove later)
			if has_acct(acct,e):
				entriesbyacct[acct].append(e)
				is_common=False
		if is_common:
			common.append(e)

	# this injects possible missing transactions for transfers 
	# between accounts
	all_splits = []
	for acct_idx, acct in enumerate(entriesbyacct):
		splits, removes = fix_transfers(entriesbyacct[acct])
		all_splits.extend(splits)
		# remove entries right away from all unseen (so far) ledgers
		# otherwise these will generate false transactions
		if acct_idx < len(entriesbyacct)-1:
			for e in removes:
				for entries in list(entriesbyacct.values())[acct_idx+1:]:
					remove_entry(e, entries)

	# place splits in proper ledger
	for acct in entriesbyacct:
		for e in all_splits:
			if has_acct(acct, e):
				entriesbyacct[acct].append(e)
	
	# now write out all the separate ledgers including common and master	
	make_path(staging_path)
	with open(os.path.join(staging_path,"common.bc"),'w') as f:
		printer.print_entries(common,file=f)

	for acct in ledgersbyacct:
		with open(os.path.join(staging_path,ledgersbyacct[acct]+"_rc.bc"),'w') as f:
			f.write('plugin "beancount.plugins.auto"\n')	
			f.write('option "booking_method" "FIFO"\n')	
			f.write('include "common.bc"\n\n')	
			printer.print_entries(entriesbyacct[acct],file=f)
	
	with open(os.path.join(staging_path,"master.bc"),'w') as f:
		f.write("; auto-generated master file {0}\n".format(dt.now().isoformat()))
		f.write('plugin "beancount.plugins.auto"\n')	
		f.write('option "booking_method" "FIFO"\n')	
		f.write('include "common.bc"\n')	
		for acct in ledgersbyacct:
			f.write('include "{0}"\n'.format(ledgersbyacct[acct]+'.bc'))


	return

##### Start of script

# typical workflow is extract -> update -> clean -> remove
# probably should combine clean and remove
if clargs.extract or clargs.check:
	extract_files()
if clargs.update:
	update_files()
if clargs.clean:
	clean_yaml()
if clargs.remove and not clargs.update:
	remove_marked()

# some other useful functions
if len(clargs.split) > 0:
	split_ledger()
if clargs.last:
	print_last()

