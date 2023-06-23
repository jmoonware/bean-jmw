# sort util for yaml files 

import yaml
import argparse
import sys
import re
from collections import OrderedDict

ap=argparse.ArgumentParser()

ap.add_argument("--existing","-e",required=True,help='Existing yaml file for account',default='')
ap.add_argument("--add","-a",required=False,help='More entries to add, usually from unassigned file',default='')
ap.add_argument("--override","-o",required=False,help='Use these entries to override any unassigned in existing',default='')
ap.add_argument("--column_width","-cw",required=False,help='Align second column to this value',default=6)
ap.add_argument("--overwrite","-ow",required=False,help='Quietly overwrite entries that are added, otherwise error if in existing',default=False,action='store_true')
ap.add_argument("--similar","-sp",required=False,help='Check regex patterns for subsetting',default=False,action='store_true')

clargs=ap.parse_args(sys.argv[1:])

# loads a dict of possible in-line comments in the existing file
# these contain date, amount info for unassigned transactions
def load_comments(f):
	retc={}
	f.seek(0)
	lines=f.readlines()
	for l in lines:
		sl=l.strip()
		if len(sl)>0 and sl[0]=='#':
			continue
		toks=sl.split(': ')
		if len(toks)>1:
			pc=toks[1].split('#') # this line has a comment
			if len(pc) > 1:
				if re.match("[0-9]+$",toks[0]): # a numerical value 
					k=int(toks[0])
				else: # quoted regex string 
					k=toks[0][1:-1] # remove quotes
				retc[k]=pc[1]

	sys.stderr.write("Found {0} comments\n".format(len(retc)))
	return(retc)

with open(clargs.existing,'r') as f:
	ex_entries=yaml.safe_load(f)
	comments_dict=load_comments(f)

add_entries={}
if len(clargs.add)>0:
	with open(clargs.add,'r') as f:
		add_entries=yaml.safe_load(f)
	for ae in add_entries:
		if ae in ex_entries and not clargs.overwrite:
			msg="Duplicate entry {0} for existing {1} (trying to add {2})\n".format(ae,ex_entries[ae],add_entries[ae])
			raise ValueError(msg)
		ex_entries[ae]=add_entries[ae] 

override_entries={}
if len(clargs.override)>0:
	with open(clargs.override,'r') as f:
		override_entries=yaml.safe_load(f)

# override logic
for oe in override_entries:
	if oe in ex_entries:
		if "UNASSIGNED" in ex_entries[oe]:
			ex_entries[oe]=override_entries[oe] 

sorted_entries=OrderedDict(sorted(ex_entries.items()))

n_int=sum([True for k in sorted_entries if type(k)==int])
if n_int/len(sorted_entries) > 0.99: # all integers
	col_width=6
	quotes=""
else:
	col_width=40
	quotes="\""

if not clargs.similar:	
	# output combined, sorted items, preserving comments
	for k,v in sorted_entries.items():
		nspace=max(col_width-len(str(k)),1)
		comment=""
		if k in comments_dict and v=="UNASSIGNED":
			comment=' # '+comments_dict[k]
		s_out = quotes + str(k) + quotes + ":" + " "*nspace + v + comment
		print(s_out)

# find out if any regexes are a subset of one another...
if clargs.similar:
	similar_patterns={}
	for p1 in sorted_entries:
		similar_patterns[p1]=[]
		for p2 in sorted_entries:
			if p1!=p2 and re.search(p1,p2):
				similar_patterns[p1].append(p2)
	
	for p1 in similar_patterns:
		if len(similar_patterns[p1])>0:
			print(p1,similar_patterns[p1])
