import argparse 
import shutil
import sys, os
import glob
import subprocess

sys.path.insert(0, os.path.abspath(os.curdir))
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

ledger_path = ".."
# relative to ledger_path
backup_folder = "backup"
bc_ext = ".txt" # beancount file extension

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

try:
	from accts import ledgersbyacct
except ImportError as ie:
	sys.stderr.write("Can't import ledgersbyacct from accts: {0}\n".format(ie))
	sys.exit(1)


ap = argparse.ArgumentParser()

ap.add_argument("-c","--check",required=False,help="Only run bean-check",default=False,action="store_true")
ap.add_argument("-e","--extract",required=False,help="Extract latest downloads, make release candidates, check",default=False,action="store_true")
ap.add_argument("--clean",required=False,help="Clean up yaml files, update chart of accounts",default=False,action="store_true")
ap.add_argument("-v","--verbose",required=False,help="Print all details",default=False,action="store_true")
ap.add_argument("--update",required=False,help="Backup orig ledger, move orig to delete file, move release candidate to orig",default=False,action="store_true")
ap.add_argument("--remove",required=False,help="Remove any undeleted safety files",default=False,action="store_true")
ap.add_argument("--test",required=False,help="Just print commands to be executed, but don't actually do anything",default=False,action="store_true")
ap.add_argument("--force",required=False,help="Force file update even if bean-check fails",default=False,action="store_true")

clargs = ap.parse_args(sys.argv[1:])

def is_nothing(fn):
	with open(fn) as f:
		lines=f.readlines()
	nothing = False
	for l in lines:
		if "Nothing to do" in l:
			nothing = True
	return(nothing)

def check(acct,opath):
	passed = False
	p = subprocess.Popen(["bean-check",opath],stderr=subprocess.PIPE)
	lines = p.stderr.readlines()
	if len(lines) > 0:
		nerr = len([x for x in lines if opath in x.decode('utf-8')])
		print(bcolors.FAIL+ "Errors for {0}, {1}: {2}".format(acct,opath,nerr) + bcolors.ENDC)
		if clargs.verbose:
			for x in lines:
				l = x.decode('utf-8').strip()
				if len(l) > 0:
					print(l)
	else:
		print(bcolors.OKGREEN+"Check passed for {0}, {1}".format(acct,opath)+bcolors.ENDC)
		passed=True
	return(passed)

def get_filenames(acct, ledger):
	epath = os.path.join(ledger_path,ledger + bc_ext)
	npath = ledger + "_new" + bc_ext
	opath = ledger + "_rc" + bc_ext
	bpath = os.path.join(ledger_path,backup_folder,ledger)+bc_ext + ".bak"
	return epath, npath, opath, bpath

def remove_marked():
	deletes = glob.glob(os.path.join(ledger_path,"*"+delete_suffix))
	problem = False
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
					problem = True
		else:
			sys.stderr.write("Found some undeleted files {0}\n".format(deletes))
			sys.stderr.write("Remember to delete {0} files manually\n".format(delete_suffix))
	return(problem)

def extract_files():
	for acct in ledgersbyacct:
		epath, npath, opath, bpath = get_filenames(acct, ledgersbyacct[acct])
		if os.path.isfile(epath):
			ecom = "python -m beanjmw.bci extract -e {0} -a {1} > {2}".format(epath,acct,npath)
		else:
			ecom = "python -m beanjmw.bci extract -a {0} > {1}".format(acct,npath)
		if not clargs.check:
			if clargs.test:
				print(ecom)
			else:
				os.system(ecom)
			if is_nothing(npath):
				print(bcolors.OKBLUE+"Nothing to do for {0}, {1}".format(acct,npath)+bcolors.ENDC)
			else:
				if os.path.isfile(epath):
					ccom = "cat {0} {1} > {2}".format(epath,npath,opath)
				else:
					ccom = "cat {0} > {1}".format(npath,opath)
				if clargs.test:
					print(ccom)
				else:
					os.system(ccom)
		# do a check of result
		passed = check(acct,opath)
	return

# this backs up old ledger, writes new ledger from release candidate
def update_files():
	# don't proceed if this fails
	if remove_marked():
		sys.exit(1)

	for acct in ledgersbyacct:
		epath, npath, opath, bpath = get_filenames(acct, ledgersbyacct[acct])
		if not check(acct,opath):
			sys.stderr.write("Failed check {0}, {1}\n".format(acct,opath))
			if not clargs.force:
				sys.stderr.write("Use --force to override\n")
				return
			
		

	# now actually move the files around	
	ok_remove = True
	for acct in ledgersbyacct:
		epath, npath, opath, bpath = get_filenames(acct, ledgersbyacct[acct])
	
		# make sure the backup folder exists or can be created
		bdir = os.path.dirname(bpath)
		if not os.path.isdir(bdir):
			try:
				if clargs.test:
					print("mkdir {0}".format(bdir))
				else:
					os.makedirs(bdir)
			except Exception as ex:
				sys.stderr.write("Problem creating {0} - goodbye: {1}\n".format(bdir,ex))
				sys.exit(1)
		# we have an existing ledger - make a backup
		if os.path.isfile(epath):
			nbpath = str(bpath)
			if os.path.isfile(bpath):
				version=0
				nbpath = '.'.join([bpath,str(version)])
				while os.path.isfile(nbpath):
					version+=1
					nbpath = '.'.join([bpath,str(version)])
			if clargs.test:
				print("cp {0} {1}".format(epath,nbpath))
			else:
				shutil.copy(epath, nbpath)
			if os.path.isfile(nbpath) and os.path.getsize(nbpath)==os.path.getsize(epath):
				# move existing ledger to a file marked for deletion
				try:
					if clargs.test:
						print("mv {0} {1}".format(epath,epath+delete_suffix))
					else:
						shutil.move(epath, epath + delete_suffix)
				except Exception as ex:
					sys.stderr.write("Can't move {0} to {0}{1}: {2}\n".format(epath,delete_suffix,ex))
					ok_remove = False
		# move release candidate to current active ledger file
		if not os.path.isfile(epath):
			try:
				if clargs.test:
					print("mv {0} {1}".format(opath,epath))
				else:
					shutil.move(opath, epath)
			except Exception as ex:
				sys.stderr.write("Can't move {0} to {1}: {2}\n".format(opath,epath,ex))
				ok_remove = False
		else:
			sys.stderr.write("File exists - can't update {0}\n".format(epath))
			ok_remove = False
	# finally, clean up marked files
	if ok_remove:
		remove_marked()
	return

def clean_yaml():
	return

##### Start of script

if clargs.extract or clargs.check:
	extract_files()
if clargs.update:
	update_files()
if clargs.remove and not clargs.update:
	remove_marked()
if clargs.clean:
	clean_yaml()
