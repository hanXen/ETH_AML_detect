from neo4j import GraphDatabase
import csv
import math
import os

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))


def push_data(start,end):
    for i in range(start,end+1):
        f = open('tx_data'+str(i)+'.csv', 'r', encoding='utf-8')
        print(f)
        rdr = csv.reader(f)
        next(rdr)
        idx = 0
        for line in rdr:
            with driver.session() as session:
                 session.write_transaction(add_account, line[1], line[2], line[0], line[3], line[4])
            print("idx: ",idx, " ,hash: ",line[0])
            idx += 1

def add_account(tx, addr, to_account, tx_hash, value, timestamp):
    if(to_account != ''):
        tx.run("MERGE (a:Account {addr: $addr, check: False, exchange: False})"
            "MERGE (b:Account {addr: $to_account, check: False, exchange: False})"
            "MERGE (a)-[:TX {tx_hash: $tx_hash, value: $value, from_addr: $addr, to_addr: $to_account, timestamp: $timestamp}]->(b)",
            addr=addr, to_account=to_account, tx_hash=tx_hash, value=float(value), timestamp=int(timestamp))

def check_exchange(tx):
    for record in tx.run("MATCH (p:Account) RETURN p.addr, size((p)-[:TX]->()) AS degree"):
        if record['degree']>1000:
            tx.run("MATCH (p:Account) WHERE p.addr=$addr SET p.exchange=True, p.degree=$degree", addr=record['p.addr'], degree=record['degree'])
            
def check_tx(tx):
    for record in tx.run("MATCH (p1:Account)-[tx1:TX]->(p2:Account)-[tx2:TX]->(p3:Account) WHERE tx1.value>35 and (tx2.value*10/tx1.value)>8 and (tx2.value/tx1.value) <1 and p1.exchange=False and p2.exchange=False  RETURN p1.addr, p2.addr, p3.addr, tx1.tx_hash, tx2.tx_hash"):
        tx.run("MERGE (a:Account {addr: $addr1, check: True})"
           "MERGE (b:Account {addr: $addr2, check: True})"
           "MERGE (c:Account {addr: $addr3, check: True})",
           addr1=record[0], addr2=record[1], addr3=record[2])
        i=0
        for record_tx in tx.run("MATCH (a:Account {addr: $addr1, check: True})-[tx:TX]->(b:Account {addr: $addr2, check: True}) RETURN tx", addr1=record[0], addr2=record[1]):
            i=i+1
        if i==0:
            tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                   "MATCH (b:Account {addr: $addr2, check: True})"
                   "MERGE (a)-[tx:TX {tx_hash: $tx_hash}]->(b)", addr1=record[0], addr2=record[1], tx_hash=record[3])

        i=0
        for record_tx in tx.run("MATCH (a:Account {addr: $addr1, check: True})-[tx:TX]->(b:Account {addr: $addr2, check: True}) RETURN tx", addr1=record[1], addr2=record[2]):
            i=i+1
        if i==0:
            tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                   "MATCH (b:Account {addr: $addr2, check: True})"
                   "MERGE (a)-[tx:TX {tx_hash: $tx_hash}]->(b)", addr1=record[1], addr2=record[2], tx_hash=record[4])
            
def check_tx_new(tx):
    sum=0
    value=1
    hash=0
    count=0
    tx.run("MATCH (s) where s.check= True DETACH DELETE s")
    print("Check node Delete complete")
    for record in tx.run("MATCH (p1:Account)-[tx1:TX]->(p2:Account)-[tx2:TX]->(p3:Account) WHERE tx1.value>35 and tx2.value >10 and p1.exchange=False and p2.exchange=False RETURN tx1.tx_hash, tx1.value, tx2.value"):
        print(record)
        if hash!=record['tx1.tx_hash']:
            if (sum<value) & ((sum*10/value)>8):
                for record_check in tx.run("MATCH (p1:Account)-[tx1:TX {tx_hash: $tx_hash}]->(p2:Account)-[tx2:TX]->(p3:Account) where tx2.value > 10 RETURN p1.addr, p2.addr, p3.addr, tx1.tx_hash, tx2.tx_hash, tx1.value, tx2.value, tx1.timestamp, tx2.timestamp", tx_hash=hash):
                    tx.run("MERGE (a:Account {addr: $addr1, check: True, income: $zero, outcome: $zero})"
                           "MERGE (b:Account {addr: $addr2, check: True, income: $zero, outcome: $zero})"
                           "MERGE (c:Account {addr: $addr3, check: True, income: $zero, outcome: $zero})",
                           addr1=record_check[0], addr2=record_check[1], addr3=record_check[2], zero=float(0))
                    i=0
                    for record_tx in tx.run("MATCH (a:Account {addr: $addr1, check: True})-[tx:TX]->(b:Account {addr: $addr2, check: True}) RETURN tx.weight", addr1=record_check[0], addr2=record_check[1]):
                        i=i+1
                    if i==0:
                        tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                               "MATCH (b:Account {addr: $addr2, check: True})"
                               "MERGE (a)-[tx:TX {tx_hash: $tx_hash, value: $value, from_addr: $addr1, to_addr: $addr2, timestamp: $timestamp, weight: $one}]->(b)", addr1=record_check[0], addr2=record_check[1], tx_hash=record_check[3], value = record_check[5], timestamp = record_check[7], one=float(1))
                    else:
                        tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                               "MATCH (b:Account {addr: $addr2, check: True})"
                               "MATCH (a)-[tx:TX {tx_hash: $tx_hash, value: $value, from_addr: $addr1, to_addr: $addr2, timestamp: $timestamp}]->(b)"
                               "SET tx.weight=$weight", addr1=record_check[0], addr2=record_check[1], tx_hash=record_check[3], value = record_check[5], timestamp = record_check[7] ,weight=record_tx["tx.weight"]+1)

                    i=0
                    for record_tx in tx.run("MATCH (a:Account {addr: $addr1, check: True})-[tx:TX]->(b:Account {addr: $addr2, check: True}) RETURN tx.weight", addr1=record_check[1], addr2=record_check[2]):
                        i=i+1
                    if i==0:
                        tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                               "MATCH (b:Account {addr: $addr2, check: True})"
                               "MERGE (a)-[tx:TX {tx_hash: $tx_hash, value: $value, from_addr: $addr1, to_addr: $addr2, timestamp: $timestamp, weight: $one}]->(b)", addr1=record_check[1], addr2=record_check[2], tx_hash=record_check[4], value = record_check[6],  timestamp = record_check[8], one = float(1))
                    else:
                        tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                               "MATCH (b:Account {addr: $addr2, check: True})"
                               "MATCH (a)-[tx:TX {tx_hash: $tx_hash, value: $value, from_addr: $addr1, to_addr: $addr2, timestamp: $timestamp}]->(b)"
                               "SET tx.weight=$weight", addr1=record_check[1], addr2=record_check[2],  tx_hash=record_check[4], value = record_check[6],  timestamp = record_check[8], weight=record_tx["tx.weight"]+float(1/i))           
            hash = record['tx1.tx_hash']
            sum=record['tx2.value']
            value=record['tx1.value']
            count=1
        else:
            x=record['tx2.value']
            sum+=x
            count+=1
            if (x<=value) & ((x*10/value)>8):
                for record_check in tx.run("MATCH (p1:Account)-[tx1:TX {tx_hash: $tx_hash1}]->(p2:Account)-[tx2:TX {tx_hash: $tx_hash2}]->(p3:Account) RETURN p1.addr, p2.addr, p3.addr, tx1.tx_hash, tx2.tx_hash, tx1.value, tx2.value, tx1.timestamp, tx2.timestamp", tx_hash1=hash, tx_hash2 = record['tx2.tx_hash']):

                    tx.run("MERGE (a:Account {addr: $addr1, check: True, icome: $zero, outcome: $zero})"
                           "MERGE (b:Account {addr: $addr2, check: True, icome: $zero, outcome: $zero})"
                           "MERGE (c:Account {addr: $addr3, check: True, icome: $zero, outcome: $zero})",
                           addr1=record_check[0], addr2=record_check[1], addr3=record_check[2], zero=float(0))
                    i=0
                    for record_tx in tx.run("MATCH (a:Account {addr: $addr1, check: True})-[tx:TX]->(b:Account {addr: $addr2, check: True}) RETURN tx.weight", addr1=record_check[0], addr2=record_check[1]):
                        i=i+1
                    if i==0:
                        tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                               "MATCH (b:Account {addr: $addr2, check: True})"
                               "MERGE (a)-[tx:TX {tx_hash: $tx_hash, value: $value, from_addr: $addr1, to_addr: $addr2, timestamp: $timestamp, weight: $one}]->(b)", addr1=record_check[0], addr2=record_check[1], tx_hash=record_check[3], value = record_check[5], timestamp = record_check[7], one=float(1))

                    else:
                        tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                               "MATCH (b:Account {addr: $addr2, check: True})"
                               "MATCH (a)-[tx:TX {tx_hash: $tx_hash, value: $value, from_addr: $addr1, to_addr: $addr2, timestamp: $timestamp}]->(b)"
                               "SET tx.weight=$weight", addr1=record_check[0], addr2=record_check[1], tx_hash=record_check[3], value = record_check[5], timestamp = record_check[7], weight=record_tx["tx.weight"]+1)

                    i=0
                    for record_tx in tx.run("MATCH (a:Account {addr: $addr1, check: True})-[tx:TX]->(b:Account {addr: $addr2, check: True}) RETURN tx.weight", addr1=record_check[1], addr2=record_check[2]):
                        i=i+1
                    if i==0:
                        tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                               "MATCH (b:Account {addr: $addr2, check: True})"
                               "MERGE (a)-[tx:TX {tx_hash: $tx_hash, value: $value, from_addr: $addr1, to_addr: $addr2, timestamp: $timestamp, weight: $one}]->(b)", addr1=record_check[1], addr2=record_check[2], tx_hash=record_check[4], value = record_check[6],  timestamp = record_check[8], one = float(1))

                    else:
                        tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                               "MATCH (b:Account {addr: $addr2, check: True})"
                               "MATCH (a)-[tx:TX {tx_hash: $tx_hash, value: $value, from_addr: $addr1, to_addr: $addr2, timestamp: $timestamp}]->(b)"
                               "SET tx.weight=$weight", addr1=record_check[1], addr2=record_check[2],  tx_hash=record_check[4], value = record_check[6],  timestamp = record_check[8], weight=record_tx["tx.weight"]+float(1/count))           
        

def weight_tx(tx):
    for record in tx.run("MATCH (p1:Account {check: True})-[tx:TX]->(p2:Account {check: True}) RETURN p1.addr, p2.addr, p1.outcome, p2.income, tx.weight"):
        outc=record['p1.outcome']+record['tx.weight']
        inc=record['p2.income']+record['tx.weight']
        tx.run("MATCH (p1:Account {addr: $addr1, check: True})-[tx:TX]->(p2:Account {addr: $addr2, check:True})"
               "SET p1.outcome=$outcome " "SET p2.income=$income", addr1=record['p1.addr'], addr2=record['p2.addr'], outcome=outc, income=float(inc))
        print(outc, inc)
    for record_bp in tx.run("MATCH (p:Account {check: True}) RETURN p.addr, p.income, p.outcome"):
        x=float(record_bp['p.income'])
        y=float(record_bp['p.outcome'])
        print("x: ", x, " y: ", y, " addr:", record_bp['p.addr'])

        if (x == 0) or (y == 0):
            print(record_bp['p.addr']+", "+str(x)+", "+str(y))
            continue
        
        bp=(2*x*y/(pow(x, 2)+pow(y, 2)))*math.log(min(x, y))
       
        tx.run("MATCH (p:Account {addr: $addr, check: True}) SET p.bp=$bp", addr=record_bp['p.addr'], bp=float(bp))

def main():
    push_data(2,5)
    #with driver.session() as session:
    #    session.read_transaction(check_exchange)
    #    session.read_transaction(check_tx_new)
    #    session.read_transaction(weight_tx)
        
if __name__ == '__main__':
    main()




