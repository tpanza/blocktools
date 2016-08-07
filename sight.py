#!/usr/bin/python
import argparse
import os
import subprocess
from blocktools import *
from block import Block, BlockHeader

def parse(blockchain, block_counter_start=0, debug=False):
	if debug: print 'print Parsing Block Chain'
	continueParsing = True
	counter = block_counter_start
	blockchain.seek(0, 2)
	fSize = blockchain.tell() - 80 #Minus last Block header size for partial file
	blockchain.seek(0, 0)
	while continueParsing:	
		block = Block(blockchain, counter, debug)
		continueParsing = block.continueParsing
		if continueParsing:
			if debug: block.toString()
			block.toCSV()
		counter+=1

	if debug: print ''
	if debug: print 'Reached End of Field'
	if debug: print 'Parsed %s blocks', counter

def main(debug, start_file_num):
	print "Running with debug=",debug, "start_file_num=",start_file_num
	end_file_num = 589 # TODO: Find # of largest complete blk*.dat. Hard code for now.
	for datfilenum in range(start_file_num, end_file_num+1):
		blk_filename = 'blk' + '{0:05d}'.format(datfilenum) + '.dat'
		if not os.path.isfile(blk_filename):
			subprocess.call(['aws', 's3', 'cp', 's3://w205-project/btc/'+blk_filename, '.'])
		num_bytes = os.path.getsize(blk_filename)
		if num_bytes < 126*1024*1024:
			print "Error:", blk_filename, "exists but is less than 126 MB"
			raise SystemExit
		print "Processing:", blk_filename, "(", num_bytes, " bytes)"
		if datfilenum == 0:
			if os.path.isfile('blocks.csv'):
				print "Error: blocks.csv already exists but we are processing first dat file"
				raise SystemExit
			with open(blk_filename, 'rb') as blockchain:
				parse(blockchain, block_counter_start=0, debug=debug)
		elif datfilenum > 0 and os.path.isfile('blocks.csv'):
			last_line = subprocess.check_output(['tail', '-n', '1', 'blocks.csv']).rstrip()
			last_block_num = int(last_line.split(',')[0])
			with open(blk_filename, 'rb') as blockchain:
				parse(blockchain, block_counter_start=last_block_num+1, debug=debug)
		else:
			print "Nothing to do. datfilenum:", datfilenum, "blk_filename:", blk_filename, "(", num_bytes, " bytes), blocks.csv exists?", os.path.isfile('blocks.csv')
		print "Deleting:", blk_filename
		os.remove(blk_filename)

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("-d", "--debug", help="Print debug output to stdout", action="store_true")
	parser.add_argument("-s", "--start-num", \
		help="Suffix of first blk*.dat file to process. Defaults to 0 (blk00000.dat)", \
		action="store", dest='start_file_num', default=0, type=int)
	args = parser.parse_args()
	main(debug=args.debug, start_file_num=args.start_file_num)
