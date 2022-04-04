import happybase

filepath = "/home/hadoop/ucl_lookup.csv"

print("Connection to HBase...")

con = happybase.Connection("localhost")
con.open()

print("Getting the table...")
htable = con.table('lookup_ucl_govind')

print("Opening the file...")

with open(filepath) as fp:
    line = fp.readline()
    line = line.strip()
    print(line)
    while line:
        print(line)
        vals = line.split(",")
        if vals[0] != 'card_id':
            htable.put(bytes(vals[0] + vals[1] + vals[2] + vals[3], encoding='utf8'),
                       {b'cf:card_id': bytes(vals[0], encoding='utf8'),
                        b'cf:UCL': bytes(vals[1], encoding='utf8'),
                        b'cf:postcode': bytes(vals[2], encoding='utf8'),
                        b'cf:transaction_dt': bytes(vals[3], encoding='utf8'),
                        b'cf:score': bytes(vals[4], encoding='utf8')})

        line = fp.readline()

print("Close Connection...")
con.close()
