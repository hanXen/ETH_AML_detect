from neo4j import GraphDatabase
import csv
import math

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))

def add_account(tx, addr, to_account, tx_hash, value, timestamp):
    tx.run("MERGE (a:Account {addr: $addr, check: False})"
           "MERGE (b:Account {addr: $to_account, check: False})"
           "MERGE (a)-[:TX {tx_hash: $tx_hash, amount: $value, timestamp: $timestamp}]->(b)",
           addr=addr, to_account=to_account, tx_hash=tx_hash, value=float(value), timestamp=int(timestamp))
def check_exchange(tx):
    for record in tx.run("MATCH (p:Account) RETURN p.addr, size((p)-[:TX]->()) AS degree"):
        if record['degree']>1000:
            tx.run("MATCH (p:Account) WHERE p.addr=$addr SET p.exchange=True, p.degree=$degree", addr=record['p.addr'], degree=record['degree'])
            
def check_tx(tx):
    for record in tx.run("MATCH (p1:Account)-[tx1:TX]->(p2:Account)-[tx2:TX]->(p3:Account) WHERE tx1.amount>35 and (tx2.amount*10/tx1.amount)>8 and (tx2.amount/tx1.amount) <1 and p1.exchange=False and p2.exchange=False  RETURN p1.addr, p2.addr, p3.addr, tx1.tx_hash, tx2.tx_hash"):
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
    for record in tx.run("MATCH (p1:Account)-[tx1:TX]->(p2:Account)-[tx2:TX]->(p3:Account) WHERE tx1.amount>35 and tx2.amount >10 and p1.exchange=False and p2.exchange=False RETURN tx1.tx_hash, tx1.amount, tx2.amount"):
        print(record)
        if hash!=record['tx1.tx_hash']:
            if (sum<value) & ((sum*10/value)>8):
                for record_check in tx.run("MATCH (p1:Account)-[tx1:TX {tx_hash: $tx_hash}]->(p2:Account)-[tx2:TX]->(p3:Account) where tx2.amount > 10 RETURN p1.addr, p2.addr, p3.addr, tx1.tx_hash, tx2.tx_hash, tx1.amount, tx2.amount", tx_hash=hash):
                    tx.run("MERGE (a:Account {addr: $addr1, check: True, income: 0.0, outcome: 0.0})"
                           "MERGE (b:Account {addr: $addr2, check: True, income: 0.0, outcome: 0.0})"
                           "MERGE (c:Account {addr: $addr3, check: True, income: 0.0, outcome: 0.0})",
                           addr1=record_check[0], addr2=record_check[1], addr3=record_check[2])
                    i=0
                    '''for record_tx in tx.run("MATCH (a:Account {addr: $addr1, check: True})-[tx:TX]->(b:Account {addr: $addr2, check: True}) RETURN tx.weight", addr1=record_check[0], addr2=record_check[1]):
                        i=i+1'''
                    if i==0:
                        tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                               "MATCH (b:Account {addr: $addr2, check: True})"
                               "MERGE (a)-[tx:TX {tx_hash: $tx_hash, weight: 1.0, amount: $amount, from_addr: $from_addr, to_addr: $to_addr}]->(b)", addr1=record_check[0], addr2=record_check[1], tx_hash=record_check[3], amount = record_check[5], from_addr = record_check[0], to_addr = record_check[1])

                    i=0
                    '''for record_tx in tx.run("MATCH (a:Account {addr: $addr1, check: True})-[tx:TX]->(b:Account {addr: $addr2, check: True}) RETURN tx.weight", addr1=record_check[1], addr2=record_check[2]):
                        i=i+1'''
                    if i==0:
                        tx.run("MATCH (a:Account {addr: $addr1, check: True})"
                               "MATCH (b:Account {addr: $addr2, check: True})"
                               "MERGE (a)-[tx:TX {tx_hash: $tx_hash, weight: 1, amount: $amount, from_addr: $from_addr, to_addr: $to_addr}]->(b)", addr1=record_check[1], addr2=record_check[2], tx_hash=record_check[4], amount = record_check[6], from_addr = record_check[1], to_addr = record_check[2])
            hash = record['tx1.tx_hash']
            sum=record['tx2.amount']
            value=record['tx1.amount']
            count=1
        else:
            sum+=record['tx2.amount']
            count+=1

def weight_tx(tx):
    for record in tx.run("MATCH (p1:Account {check: True})-[tx:TX]->(p2:Account {check: True}) RETURN p1.addr, p2.addr, p1.outcome, p2.income, tx.weight"):
        tx.run("MATCH (p1:Account {addr: $addr1, check: True}-[tx:TX]->(p2:Account {addr: $addr2, check:True}) SET p1.outcome=$outcome, p2.income=$income", record['p1.addr'], record['p2.addr'], record['p1.outcome']+record['tx.weight'], record['p2.income']+record['tx.weight'])

    for record_bp in tx.run("MATCH (p:Account {check: True}) RETURN p.addr, p.income, p.outcome"):
        x=record_bp['p.income']
        y=record_bp['p.outcome']
        bp=2*x*y/(pow(x, 2)+pow(y, 2))*math.log(min(x, y))
        tx.run("MATCH (p:Account {addr: $addr, check: True}) SET p.bp=$bp", record['p.addr'], bp)
'''
for i in range(1, 2):
    f = open('tx_data'+str(i)+'.csv', 'r', encoding='utf-8')
    rdr = csv.reader(f)
    next(rdr)
    for line in rdr:
        with driver.session() as session:
            session.write_transaction(add_account, line[1], line[2], line[0], line[3], line[4])

f.close()
'''
with driver.session() as session:
    session.read_transaction(check_exchange)
    session.read_transaction(check_tx_new)


