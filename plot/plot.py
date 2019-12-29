import psycopg2
import matplotlib.pyplot as plt
import numpy as np

# def fetch():
#     conn = psycopg2.connect(host='165.22.62.101',
#                             port="5432",
#                             database="bitmex", 
#                             user="jakeadelman", 
#                             password="jakeadelman")
#     c = conn.cursor()
#     SQL = "select * from liquidity;"


#     res = c.execute(SQL)
#     print(res)

def fetch():
    conn = None
    mins = (24*60)/5

    try:
        conn = psycopg2.connect(host='165.22.62.101',
                            port="5432",
                            database="bitmex", 
                            user="jakeadelman", 
                            password="jakeadelman")
        cur = conn.cursor()
        cur.execute("SELECT id, openinterest, long, short, bpa, spa, avgprice, timestamp FROM liquidity WHERE"+
        " id>120 ORDER BY id DESC")
        rows = cur.fetchall()
        fin = []
        ids = []
        openinterest = []
        longs = []
        short = []
        bpa = []
        spa = []
        avgprice = []
        timestamp =[]
        longshort = 0
        print("The number of rows: ", cur.rowcount)
        for row in rows:
            # print(row)
            
            try:
                OI1 = openinterest[-1]
                
                # initchange = row[1]-OI1
                # longshort = row[2]/row[3]
                openinterest.append(row[1])
                ids.append(row[0])
                # longchange = initchange*longshort
                # newlong = longs[-1]+longchange
                # shortchange = initchange*(1-longshort)
                # newshort = short[-1]+shortchange
               
                longs.append(row[2])            
                short.append(row[3]) 
                # print(len(bpa))
               
                bpa.append(row[4])
                spa.append(row[5])
                avgprice.append(row[6])
                timestamp.append(row[7])           
            except:
                # print("out")
                ids.append(row[0])
                openinterest.append(row[1]) 
                longdiv = (row[1]//2)
                longs.append(longdiv)
                short.append(longdiv)
                bpa.append(row[4])
                spa.append(row[5])
                avgprice.append(row[6])
                timestamp.append(row[7])
            
        ids.reverse()
        fin.append(ids)
        openinterest.reverse()
        fin.append(openinterest)
        longs.reverse()
        fin.append(longs)
        short.reverse()
        fin.append(short)
        bpa.reverse()
        fin.append(bpa)
        spa.reverse()
        fin.append(spa)
        avgprice.reverse()
        fin.append(avgprice)
        timestamp.reverse()
        fin.append(timestamp)
        return(fin)



        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_avg(bpa,spa):
    fin = []
    bpafin = []
    spafin = []
    j=0
    while j<len(bpa):
        bpatot = 0
        spatot = 0
        if j==0:
            bpafin.append(bpa[0])
            spafin.append(spa[0])
        else:
            for i in range(j):
                bpatot+=bpa[i+1]

            for i in range(j):
                spatot+=spa[i+1]
            
            # bpatot+=bpa[j]
            # spatot+=spa[j]
            # print(row[4],row[5], len(short))
            bpatot = bpatot//j
            spatot = spatot//j
            print(bpatot,spatot)
            bpafin.append(bpatot)
            spafin.append(spatot)
        j+=1

    fin.append(bpafin)
    fin.append(spafin)
    return(fin)



def format_long_short(res):
    final = []
    openinterest = []
    newlongs = []
    newshorts = []
    initinterest = 0
    initchange = 0
    longshort = 0
    longchange = 0
    shortchange = 0
    newlong = 0
    newshort = 0
    longshort = 0
    opens = res[1]
    longs = res[2]
    short = res[3]

    i=0

    while i< len(opens):
        
        try:
            
            OI1 = openinterest[-1]

            initchange = opens[i]-OI1
            tot = longs[i]+short[i]
            longshort = longs[i]/tot

            openinterest.append(opens[i])
            longchange = initchange*longshort
            newlong = longs[-1]+longchange
            shortchange = initchange*(1-longshort)
            newshort = short[-1]+shortchange
            longs[i]=newlong
            short[i]=newshort
            newlongs.append(newlong)            
            newshorts.append(newshort) 
            i+=1
        except:
            openinterest.append(opens[i])
            newlongs.append(opens[i]//2)
            newshorts.append(opens[i]//2)
            i+=1

    final.append(opens)
    final.append(newlongs)
    final.append(newshorts)

    return(final)

initres = fetch()
res = format_long_short(initres)


y = np.array(initres[0])
x = np.array(initres[1])
p = np.array(initres[4])
bsavg = get_avg(initres[5], initres[6])
bp = np.array(bsavg[0])
sp = np.array(bsavg[1])
x2 = np.array(res[1])
x3 = np.array(res[2])
print("last timestamp: "+ initres[7][-1])

fig, axs = plt.subplots(3)
# fig.suptitle('Vertically stacked subplots')
axs[0].plot(y, x)
axs[1].plot(y, x2)
axs[1].plot(y, x3)
axs[2].plot(y, p)
axs[2].plot(y, bp)
axs[2].plot(y, sp)

labels=['openinterest','long/short change','price']
i=0
while i<len(axs.flat):
    axs.flat[i].set(ylabel=labels[i])
    i+=1

plt.show()
