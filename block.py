from blocktools import *
import csv

class BlockHeader:
	def __init__(self, blockchain):
		self.version = uint4(blockchain)
		self.previousHash = hash32(blockchain)
		self.merkleHash = hash32(blockchain)
		self.time = uint4(blockchain)
		self.bits = uint4(blockchain)
		self.nonce = uint4(blockchain)
	def toString(self):
		print "Version:\t %d" % self.version
		print "Previous Hash\t %s" % hashStr(self.previousHash)
		print "Merkle Root\t %s" % hashStr(self.merkleHash)
		print "Time\t\t %s" % str(self.time)
		print "Difficulty\t %8x" % self.bits
		print "Nonce\t\t %s" % self.nonce
	def getBlockHeader(self):
		return [self.version, hashStr(self.previousHash), hashStr(self.merkleHash), \
			str(self.time), hex(self.bits), self.nonce]

class Block:
	def __init__(self, blockchain, block_id):
		self.block_id = block_id
		self.continueParsing = True
		self.magicNum = 0
		self.blocksize = 0
		self.blockheader = ''
		self.txCount = 0
		self.Txs = []

		if self.hasLength(blockchain, 8):	
			self.magicNum = uint4(blockchain)
			self.blocksize = uint4(blockchain)
		else:
			self.continueParsing = False
			return
		
		if self.hasLength(blockchain, self.blocksize):
			self.setHeader(blockchain)
			self.txCount = varint(blockchain)
			self.Txs = []

			for i in range(0, self.txCount):
				tx = Tx(blockchain, fkey_block_id=self.block_id, trans_pos=i)
				self.Txs.append(tx)
		else:
			self.continueParsing = False
						

	def continueParsing(self):
		return self.continueParsing

	def getBlocksize(self):
		return self.blocksize

	def hasLength(self, blockchain, size):
		curPos = blockchain.tell()
		blockchain.seek(0, 2)
		
		fileSize = blockchain.tell()
		blockchain.seek(curPos)

		tempBlockSize = fileSize - curPos
		print tempBlockSize
		if tempBlockSize < size:
			return False
		return True


	def setHeader(self, blockchain):
		self.blockHeader = BlockHeader(blockchain)

	def toString(self):
		print ""
		print "Magic No: \t%8x" % self.magicNum
		print "Blocksize: \t", self.blocksize
		print ""
		print "#"*10 + " Block Header " + "#"*10
		self.blockHeader.toString()
		print 
		print "##### Tx Count: %d" % self.txCount
		for t in self.Txs:
			t.toString()
			t.toCSV()

	def toCSV(self, block_csv_filename="blocks.csv"):
		with open(block_csv_filename, 'a') as f:
			writer = csv.writer(f)
			writer.writerow([self.block_id, hex(self.magicNum), self.blocksize] + \
				self.blockHeader.getBlockHeader()) 

class Tx:
	def __init__(self, blockchain, fkey_block_id, trans_pos):
		self.block_id = fkey_block_id
		self.trans_pos = trans_pos
		self.version = uint4(blockchain)
		self.inCount = varint(blockchain)
		self.inputs = []
		for i in range(0, self.inCount):
			input = txInput(blockchain, self.block_id, self.trans_pos, i)
			self.inputs.append(input)
		self.outCount = varint(blockchain)
		self.outputs = []
		if self.outCount > 0:
			for i in range(0, self.outCount):
				output = txOutput(blockchain, self.block_id, self.trans_pos, i)
				self.outputs.append(output)	
		self.lockTime = uint4(blockchain)
		
	def toString(self):
		print ""
		print "="*10 + " New Transaction " + "="*10
		print "Tx Version:\t %d" % self.version
		print "Inputs:\t\t %d" % self.inCount
		for i in self.inputs:
			i.toString()
			i.toCSV()

		print "Outputs:\t %d" % self.outCount
		for o in self.outputs:
			o.toString()
			o.toCSV()
		print "Lock Time:\t %d" % self.lockTime

	def toCSV(self, transaction_csv_filename="transactions.csv"):
		with open(transaction_csv_filename, 'a') as f:
			writer = csv.writer(f)
			writer.writerow([self.block_id, self.trans_pos, self.version, self.inCount, \
				self.outCount, self.lockTime])
				

class txInput:
	def __init__(self, blockchain, fkey_block_id, fkey_trans_pos, tx_in_pos):
		self.block_id = fkey_block_id
		self.trans_pos = fkey_trans_pos
		self.tx_in_pos = tx_in_pos
		self.prevhash = hash32(blockchain)
		self.txOutId = uint4(blockchain)
		self.scriptLen = varint(blockchain)
		self.scriptSig = blockchain.read(self.scriptLen)
		self.seqNo = uint4(blockchain)

	def toString(self):
		print "Previous Hash:\t %s" % hashStr(self.prevhash)
		print "Tx Out Index:\t %8x" % self.txOutId
		print "Script Length:\t %d" % self.scriptLen
		print "Script Sig:\t %s" % hashStr(self.scriptSig)
		print "Sequence:\t %8x" % self.seqNo

	def toCSV(self, tx_in_csv_filename="tx_in.csv"):
		with open(tx_in_csv_filename, 'a') as f:
			writer = csv.writer(f)
			writer.writerow([self.block_id, self.trans_pos, self.tx_in_pos, hashStr(self.prevhash), \
				hex(self.txOutId), self.scriptLen, hashStr(self.scriptSig), hex(self.seqNo)])
		
class txOutput:
	def __init__(self, blockchain, fkey_block_id, fkey_trans_pos, tx_out_pos):
		self.block_id = fkey_block_id
		self.trans_pos = fkey_trans_pos
		self.tx_out_pos = tx_out_pos
		self.value = uint8(blockchain)
		self.scriptLen = varint(blockchain)
		self.pubkey = blockchain.read(self.scriptLen)

	def toString(self):
		print "Value:\t\t %d" % self.value
		print "Script Len:\t %d" % self.scriptLen
		print "Pubkey:\t\t %s" % hashStr(self.pubkey)
	
	def toCSV(self, tx_out_csv_filename="tx_out.csv"):
		with open(tx_out_csv_filename, 'a') as f:
			writer = csv.writer(f)
			writer.writerow([self.block_id, self.trans_pos, self.tx_out_pos, self.value, \
				self.scriptLen, hashStr(self.pubkey)])

