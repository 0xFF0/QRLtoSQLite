#!/usr/bin/env python3
# -*- coding: utf-8 -*

"""
    QRL Blockchain to SQlite 
	v1.0
	
	Requirements/Setup:
		> sudo apt-get install python3-pip
		> pip3 install qrl

"""

__author__ = ['0xFF (https://github.com/0xFF0)']
__version__ = "1.0"
__date__ = '2022.01.29'


import sqlite3
import plyvel
import argparse
import binascii
import base64
import sys
import copy
from google.protobuf.json_format import MessageToJson, Parse, MessageToDict
from qrl.generated import qrl_pb2


DB_INSERT_MAX = 100000

DB_INSERT_OPTIMIZATION_TEMPLATE = {
    "total" : 0,
    "addresses": { "query": "INSERT INTO addresses VALUES (?,?,?,?,?)", "data" : [] },
    "updateAddresses": { "query": "UPDATE addresses SET lastSeen=? WHERE address=?", "data" : [] },
    "blockMetadata": { "query": "INSERT INTO blockMetadata VALUES (?,?,?,?,?)", "data": [] },
    "otherTransactions": { "query": "INSERT INTO otherTransactions VALUES (?,?,?,?)", "data": [] },
    "messages": { "query": "INSERT INTO messages VALUES (?,?,?)", "data": [] },
    "tokens": { "query": "INSERT INTO tokens VALUES (?,?,?,?)", "data": [] }
}

DB_INSERT_OPTIMIZATION = copy.deepcopy(DB_INSERT_OPTIMIZATION_TEMPLATE)
        
TMP_ADDR_LIST = []      
          
def insertSqliteData(cur,con):
    global DB_INSERT_OPTIMIZATION
    for table in DB_INSERT_OPTIMIZATION:
        if table != "total":
            if len(DB_INSERT_OPTIMIZATION[table]["data"]) > 0:
                cur.executemany(DB_INSERT_OPTIMIZATION[table]["query"], DB_INSERT_OPTIMIZATION[table]["data"])
                
    con.commit()
    DB_INSERT_OPTIMIZATION = copy.deepcopy(DB_INSERT_OPTIMIZATION_TEMPLATE)
  

    

def createSqliteDB(stateFolder, outputDBName):

    #Create database and table
    con = sqlite3.connect(outputDBName)
    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS addresses (address text, balance real, firstSeen text, lastSeen text, tag text)''')   
    cur.execute('''CREATE TABLE IF NOT EXISTS blockMetadata (blockNum int, hashHeader text, timestampSeconds text, nbTransactions int, rewardBlock text)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS otherTransactions (blockNum int, transactionHash text, txType text, data text)''') 
    cur.execute('''CREATE TABLE IF NOT EXISTS tokens (tokenName text, tokenSymbol text, tokenOwner text, transactionHash text)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS messages (messageHash text, blockNum int, transactionHash text)''')    
    


    db = plyvel.DB(stateFolder)

    blockheight = int.from_bytes(db.get(b'blockheight'), byteorder='big', signed=False)


    pbdata = qrl_pb2.Block()
    block_number_mapping = qrl_pb2.BlockNumberMapping()
    
    print("Parsing block 0/" + str(blockheight)) 
    
    for i in range(0, blockheight):
        
        if DB_INSERT_OPTIMIZATION["total"] > DB_INSERT_MAX:
            insertSqliteData(cur,con)
            print("Parsing block " + str(i) + "/" + str(blockheight)) 
        
               
        #Get block
        hashHeader = Parse(db.get(str(i).encode()), block_number_mapping).headerhash
        pbdata.ParseFromString(bytes(db.get(hashHeader)))
        dictData = MessageToDict(pbdata)
        
        #Block Metadata
        blockNumber = i
        hashHeaderHex = hashHeader.hex()
        timestampSeconds = dictData["header"]["timestampSeconds"]
        nbTransactions = len(dictData["transactions"])
        rewardBlock = dictData["header"]["rewardBlock"]
 
        DB_INSERT_OPTIMIZATION["blockMetadata"]["data"].append([blockNumber, hashHeaderHex, timestampSeconds, nbTransactions, rewardBlock])
        DB_INSERT_OPTIMIZATION["total"] += 1

        
        
        for t in dictData["transactions"]:
            transactionProcessed = False
            
            if "coinbase" in t:
                addAddressInDB(cur, db, t["coinbase"]["addrTo"], timestampSeconds)
                transactionProcessed = True
                
            if "transfer" in t:
                for a in t["transfer"]["addrsTo"]:
                    addAddressInDB(cur, db, a, timestampSeconds)

                    
                transactionProcessed = True

            if "token" in t:
                tokenName = base64.b64decode(t["token"]["name"]).decode("utf-8") 
                tokenSymbol = base64.b64decode(t["token"]["symbol"]).decode("utf-8") 
                tokenOwner = "Q" + base64.b64decode(t["token"]["owner"]).hex()
                transactionHash = base64.b64decode(t["transactionHash"]).hex()
                DB_INSERT_OPTIMIZATION["tokens"]["data"].append([tokenName, tokenSymbol, tokenOwner, transactionHash])
                DB_INSERT_OPTIMIZATION["total"] += 1
                transactionProcessed = True

            if "message" in t:             
                try:
                    messageHash = base64.b64decode(t["message"]["messageHash"]).decode("utf-8") 
                except:
                    messageHash = base64.b64decode(t["message"]["messageHash"]).hex()
                    
                #https://github.com/theQRL/qips/blob/master/qips/QIP002.md
                if messageHash.startswith("afaf"):
                    if messageHash.startswith("afafa1"):
                        try:
                            docText = binascii.a2b_hex(messageHash[46:]).decode("utf-8") 
                        except:
                            docText = binascii.a2b_hex(messageHash[46:]).hex()
                        messageHash = "[Doc notarization] SHA1: " + messageHash[6:46] + " TEXT: " + docText 
                    elif messageHash.startswith("afafa2"):
                        try:
                            docText = binascii.a2b_hex(messageHash[70:]).decode("utf-8") 
                        except:
                            docText = binascii.a2b_hex(messageHash[70:]).hex()
                        messageHash = "[Doc notarization] SHA256: " + messageHash[6:70] + " TEXT: " + docText
                    elif messageHash.startswith("afafa3"):
                        try:
                            docText = binascii.a2b_hex(messageHash[38:]).decode("utf-8") 
                        except:
                            docText = binascii.a2b_hex(messageHash[38:]).hex()
                        messageHash = "[Doc notarization] MD5: " + messageHash[6:38] + " TEXT: " + docText   
                        
                #https://github.com/theQRL/message-transaction-encoding      
                elif messageHash.startswith("0f0f"):
                    msgHeader = "[Unknown]"
                    msgBegin = 8
                    text = ""
                    
                    if messageHash.startswith("0f0f0000") or messageHash.startswith("0f0f0001"):
                        msgHeader = "[Reserved] "
                        
                    elif messageHash.startswith("0f0f0002"): 
                        if messageHash.startswith("0f0f0002af"): 
                            msgHeader = "[Keybase-remove] "
                        elif messageHash.startswith("0f0f0002aa"): 
                            msgHeader = "[Keybase-add] "
                        else:
                            msgHeader = "[Keybase-" + messageHash[8:10] + "] "
                            
                        msgBegin = 12
                        try:
                            user = binascii.a2b_hex(messageHash[msgBegin:].split("20")[0]).decode("utf-8")
                            keybaseHex = binascii.a2b_hex(messageHash[msgBegin + len(user)*2 + 2:]).hex()
                            text = "USER: " + user + " KEYBASE_HEX: " + keybaseHex
                        except:
                            text = ""

                    elif messageHash.startswith("0f0f0003"):
                        if messageHash.startswith("0f0f0002af"): 
                            msgHeader = "[Github-remove] "
                        elif messageHash.startswith("0f0f0002aa"): 
                            msgHeader = "[Github-add] "
                        else:
                            msgHeader = "[Github-" + messageHash[8:10] + "] "
                            
                        msgBegin = 18
                        text = binascii.a2b_hex(messageHash[msgBegin:]).hex()

                    elif messageHash.startswith("0f0f0004"):
                        msgHeader = "[Vote] "                           
                                             
                    if len(text) == 0:                           
                        try:
                            text = binascii.a2b_hex(messageHash[msgBegin:]).decode("utf-8")
                        except:
                            try:
                                text = binascii.a2b_hex(messageHash[msgBegin:]).hex()
                            except:
                                text = str(messageHash[msgBegin:])
                        
                    messageHash = msgHeader + text


          
                transactionHash = base64.b64decode(t["transactionHash"]).hex()
                DB_INSERT_OPTIMIZATION["messages"]["data"].append([messageHash, blockNumber, transactionHash])
                DB_INSERT_OPTIMIZATION["total"] += 1  
                transactionProcessed = True    
                                
            if not transactionProcessed:
                txHash = base64.b64decode(t["transactionHash"]).hex()
                data = ""
                txType = ""
                
                if "slave" in t:
                    txType = "slave"
                    
                if "transferToken" in t:
                    txType = "transferToken"
                    
                if "multiSigCreate" in t:
                    txType = "multiSigCreate"

                if "latticePK" in t:
                    txType = "latticePK" 
                
                if "multiSigSpend" in t:
                    txType = "multiSigSpend"                       

                if "multiSigVote" in t:
                    txType = "multiSigVote"    
                                   
                if len(txType) == 0:
                    data = str(t)
                    

                DB_INSERT_OPTIMIZATION["otherTransactions"]["data"].append([blockNumber, txHash, txType, data])
                DB_INSERT_OPTIMIZATION["total"] += 1 
            
        
            con.commit()
    
    insertSqliteData(cur,con)   
    con.close()
    db.close()
        

def addAddressInDB(dbCursor, levelDB, b64Addr, timeStamp): 

    addrData = qrl_pb2.AddressState()
    
    addrByte = base64.b64decode(b64Addr)
    address = "Q" + addrByte.hex()
    
    try:
        updateAddr = False
            
        if address in TMP_ADDR_LIST:
            updateAddr = True
        else:
            addrData.ParseFromString(levelDB.get(addrByte))
            dictData = MessageToDict(addrData)

            if "balance" in dictData:
                balance = float(dictData["balance"])/1000000000
            else:
                balance = "0"
            
            DB_INSERT_OPTIMIZATION["addresses"]["data"].append([address, balance, timeStamp, timeStamp, ""])
            DB_INSERT_OPTIMIZATION["total"] += 1
            TMP_ADDR_LIST.append(address)
            
        if updateAddr:
            DB_INSERT_OPTIMIZATION["updateAddresses"]["data"].append([timeStamp, address])
            DB_INSERT_OPTIMIZATION["total"] += 1
    
    except:
        print("Error parsing " + address + ". Timestamp: " + timeStamp)
    
        
    


if __name__ == "__main__":
    desc = "QRL Blockchain to SQlite v" + __version__
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("-i", "--indexQRLStateFolder", help="QRL state folder (ex: /home/ubuntu/.qrl/data/state)", type=str)
    parser.add_argument("-o", "--outputDBName", help="Output QRL sqlite db name (ex: qrl.sqlite)", type=str)
    
    # Read arguments from the command line
    options = parser.parse_args()

    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)
	
    if options.indexQRLStateFolder is not None and options.outputDBName is not None:
        createSqliteDB(options.indexQRLStateFolder, options.outputDBName)
        
    else:
        parser.print_help()


