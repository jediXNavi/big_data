import sys
import os
import gzip
import re
import datetime as d
from cassandra.cluster import Cluster, BatchStatement, ConsistencyLevel

def reg_exp(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = line_re.search(line)
    if match is not None:
        a = line_re.split(line)
        b= ' '.join(a).split()
        return b

def chunks(l, n):
    list2 = []
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        list2.append(l[i:i + n])
    return list2


def insert_batch(rec,session,table_name):
    for each_list in rec:
        for list_content in each_list:
            host = list_content[0]
            datetime = d.datetime.strptime(list_content[1],"%d/%b/%Y:%H:%M:%S")
            path = list_content[2]
            byte = int(list_content[3])
            insert_statement = session.prepare("INSERT INTO %s(host,uid,datetime,path,bytes) VALUES (?,UUID(),?,?,?)"%table_name)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch.add(insert_statement, (host,datetime,path,byte))
            session.execute(batch)
            batch.clear()

def main(inputs, keyspace, table_name):
    list_data = []
    cluster = Cluster(['199.60.17.32', '199.60.17.65'])
    session = cluster.connect(keyspace)
    for f in os.listdir(inputs):
        with gzip.open(os.path.join(inputs,f),'rt',encoding='utf-8') as logfile:
            for line in logfile:
                proper_line = reg_exp(line)
                if proper_line is not None:
                   list_data.append(proper_line)
    a=chunks(list_data,100)
    insert_batch(a,session,table_name)




if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(inputs, keyspace, table_name)



