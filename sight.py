#!/usr/bin/python
import argparse
import os
import subprocess
from blocktools import *
from block import Block, BlockHeader

def parse(blockchain, block_counter_start=0, datfilenum=0, debug=False):
	if debug: print 'print Parsing Block Chain'
	continueParsing = True
	counter = block_counter_start
	blockchain.seek(0, 2)
	fSize = blockchain.tell() - 80 #Minus last Block header size for partial file
	blockchain.seek(0, 0)
	while continueParsing:	
		block = Block(blockchain, counter, datfilenum, debug)
		continueParsing = block.continueParsing
		if continueParsing:
			if debug: block.toString()
			block.toCSV(block_csv_filename='blocks'+'{0:05d}'.format(datfilenum)+'.csv')
		counter+=1

	if debug: print ''
	if debug: print 'Reached End of Field'
	if debug: print 'Parsed %s blocks', counter
	return counter

def make_stream_csv(dat_file_name, last_block_num, debug=False):
	# must be running on same machine as bitcoind client
	print "Making CSV with: dat_file_name:", dat_file_name, "last block num:", last_block_num
	datfilenum = int(filter(str.isdigit, dat_file_name))
	new_blocks_parsed = 0
	with open(dat_file_name, 'rb') as blockchain:
		new_blocks_parsed = parse(blockchain, block_counter_start=last_block_num+1, datfilenum=datfilenum, debug=debug)
	blocks_csv_filename = 'blocks'+'{0:05d}'.format(datfilenum)+'.csv'
	subprocess.call(['aws', 's3', 'cp', blocks_csv_filename, 's3://w205-project/blocks_stream/'+blocks_csv_filename])
	transaction_csv_filename='transactions'+'{0:05d}'.format(datfilenum)+'.csv'
	subprocess.call(['aws', 's3', 'cp', transaction_csv_filename, 's3://w205-project/transactions_stream/'+transaction_csv_filename])
	tx_in_csv_filename="tx_in"+'{0:05d}'.format(datfilenum)+".csv"
	subprocess.call(['aws', 's3', 'cp', tx_in_csv_filename, 's3://w205-project/tx_in_stream/'+tx_in_csv_filename])
	tx_out_csv_filename="tx_out"+'{0:05d}'.format(datfilenum)+".csv"
	subprocess.call(['aws', 's3', 'cp', tx_out_csv_filename, 's3://w205-project/tx_out_stream/'+tx_out_csv_filename])
	os.remove(blocks_csv_filename)
	os.remove(transaction_csv_filename)
	os.remove(tx_in_csv_filename)
	os.remove(tx_out_csv_filename)
	return last_block_num + new_blocks_parsed

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
				parse(blockchain, block_counter_start=0, datfilenum=datfilenum, debug=debug)
		elif datfilenum > 0 and os.path.isfile('blocks.csv'):
			last_line = subprocess.check_output(['tail', '-n', '1', 'blocks.csv']).rstrip()
			last_block_num = int(last_line.split(',')[0])
			with open(blk_filename, 'rb') as blockchain:
				parse(blockchain, block_counter_start=last_block_num+1, datfilenum=datfilenum, debug=debug)
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
