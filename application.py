from flask import Flask,render_template,request
import os
import pyodbc
import random
import redis
import time
import hashlib
from json import loads, dumps

app = Flask(__name__)



myHostname = "vijayazure.redis.cache.windows.net"
myPassword = "eGWbQ9K6RpbnGvMeGkpeylVnGwgXb4Rp6LjqTOcC9tE="

r = redis.StrictRedis(host=myHostname, port=6380, password=myPassword, ssl=True)

# result = r.ping()
# print("Ping returned : " + str(result))

# result = r.set("Message", "Hello!, The cache is working with Python!")
# print("SET Message returned : " + str(result))

# result = r.get("Message")
# print("GET Message returned : " + result.decode("utf-8"))

# result = r.client_list()
# print("CLIENT LIST returned : ")
# for c in result:
#     print("id : " + c['id'] + ", addr : " + c['addr'])

@app.route('/')
def hello_world():
    conn = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=tcp:azurevijaydb.database.windows.net,1433;Database=Quakes;Uid=vijaykant009@azurevijaydb;Pwd=J@ik@nt009;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    lstDictionaryData = []
    cursor = conn.cursor()
    startTime = time.time()
    query = "SELECT TOP 10 latitude, longitude, mag, place FROM all_month"
    # print(query)
    cursor.execute(query)
    # print(tmp)
    endTime = time.time()
    row = cursor.fetchone()
    while row:
        lstDictionaryData.append(row)
        # print("hi!" + str(row))
        row = cursor.fetchone()
    # return "hello!!"
    conn.close()
    executionTime = (endTime - startTime) * 1000
    return render_template('index.html', tableData=lstDictionaryData, tableDataLen=lstDictionaryData.__len__(), executionTime=executionTime)

@app.route('/showdb', methods=['GET', 'POST'])
def showdb():
    limit = request.form['limit']
    cnxn = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=tcp:azurevijaydb.database.windows.net,1433;Database=Quakes;Uid=vijaykant009@azurevijaydb;Pwd=J@ik@nt009;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = cnxn.cursor()
    cursor.execute("SELECT TOP "+limit+" * from all_month ")
    row = cursor.fetchall()
    return render_template("showdb.html", row=row)


@app.route('/createtable',methods=['GET', 'POST'])
def createTable():
    # lstDictionaryData = []
    conn = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=tcp:azurevijaydb.database.windows.net,1433;Database=Quakes;Uid=vijaykant009@azurevijaydb;Pwd=J@ik@nt009;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = conn.cursor()
    # query = "CREATE TABLE dbo.all_month (\"time\" datetime, \"latitude\" FLOAT, \"longitude\" FLOAT, \"depth\" FLOAT, \"mag\" FLOAT, \"magType\" TEXT, \"nst\" INT, \"gap\" INT, \"dmin\" FLOAT, \"rms\" FLOAT, \"net\" TEXT, \"id\" TEXT, \"updated\" datetime, \"place\" TEXT, \"type\" TEXT, \"horontalError\" FLOAT, \"depthError\" FLOAT, \"magError\" FLOAT, \"magNst\" INT, \"status\" TEXT, \"locationSource\" TEXT, \"magSource\" TEXT)"
    query = 'CREATE TABLE Quakes.dbo.all_month2("time" DATETIME,latitude FLOAT,longitude FLOAT,depth FLOAT,mag FLOAT,magType TEXT,nst INT,gap INT,dmin FLOAT,rms FLOAT,net TEXT,id TEXT,updated DATETIME,place TEXT,type TEXT,horontalError FLOAT,depthError FLOAT,magError FLOAT,magNst INT,status TEXT,locationSource TEXT,magSource TEXT)'
    # print(query)
    startTime = time.time()
    # cursor.execute(query)
    cursor.execute(query)
    cursor.execute("CREATE INDEX all_month_mag__index ON Quakes.dbo.all_month2 (mag)")
    cursor.execute("CREATE INDEX all_month_lat__index ON Quakes.dbo.all_month2 (latitude)")
    cursor.execute("CREATE INDEX all_month_long__index ON Quakes.dbo.all_month2 (longitude)")
    conn.commit()
    endTime = time.time()
    conn.close()
    executionTime = (endTime - startTime) * 1000
    return render_template('createtable.html', executionTime=executionTime)

@app.route('/location', methods=['GET', 'POST'])
def location():
    lat1 = request.form['lat1']
    lon1 = request.form['lon1']
    lat2 = request.form['lat2']
    lon2 = request.form['lon2']
    cnxn = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=tcp:azurevijaydb.database.windows.net,1433;Database=Quakes;Uid=vijaykant009@azurevijaydb;Pwd=J@ik@nt009;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = cnxn.cursor()
    cursor.execute("Select * from all_month where longitude >= '"+lon1+"' and longitude <= '"+lon2+"' and latitude >='"+lat1+"' and latitude <= '"+lat2+"' ")
    result = cursor.fetchall()
    print(result)
    return render_template("location.html", row=result)


@app.route('/randomqueries', methods=['GET', 'POST'])
def randomQueries():
    magnitudeStart = int(request.form['minmag'])
    magnitudeEnd = int(request.form['maxmag'])
    noOfQueries = int(request.form['count'])
    withCache = int(request.form['Cache'])
    # noOfQueries = request.form["quer"]
    # print(type(noOfQueries))
    # withCache = int(request.form['cache'])
    # magnitudeStart = float(request.form['magnitudeStart'])
    # magnitudeEnd = float(request.form['magnitudeEnd'])

    list_dict_Data = []
    list_dict_DataDisplay = []

    conn = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=tcp:azurevijaydb.database.windows.net,1433;Database=Quakes;Uid=vijaykant009@azurevijaydb;Pwd=J@ik@nt009;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = conn.cursor()
    totalExecutionTime = 0
    columns = ['time', 'latitude', 'longitude', 'place', 'mag']

    # without cache
    if withCache == 0:
        # print("hi!")

        magnitude_value = round(random.uniform(magnitudeStart, magnitudeEnd), 2)
        print(magnitude_value)
        startTime = time.time()
        query = "SELECT 'time', latitude, longitude, place, mag FROM all_month WHERE mag = '" + str(magnitude_value) + "'"
        cursor.execute(query)
        endTime = time.time()
        # print(query)
        list_dict_DataDisplay = cursor.fetchall()
        # print(list_dict_DataDisplay)
        executionTime = (endTime - startTime) * 1000
        firstExecutionTime = executionTime

        for i in range(noOfQueries-1):
            totalExecutionTime = totalExecutionTime + executionTime
            magnitude_value = round(random.uniform(magnitudeStart, magnitudeEnd), 2)
            startTime = time.time()
            query = "SELECT 'time', latitude , longitude, place, mag FROM all_month WHERE mag = '" + str(magnitude_value) + "'"
            cursor.execute(query)
            endTime = time.time()
            list_dict_Data = list(cursor.fetchall())
            # print("inside if")
            # print(lstDictionaryData)

            memData = []
            for row in list_dict_Data:
                memDataDict = dict()
                for i, val in enumerate(row):
                    # if type(val) == datetime:
                    #     val = time.mktime(val.timetuple())
                    memDataDict[columns[i]] = val
                memData.append(memDataDict)
            r.set(query, dumps(memData))

            executionTime = (endTime - startTime) * 1000
            # totalExecutionTime = totalExecutionTime + executionTime
        # print(totalExecutionTime)
    # with cache
    else:
        print('Cache inside')
        for x in range(noOfQueries):
            print('x')
            print(x)
            magnitude_value = round(random.uniform(magnitudeStart, magnitudeEnd), 2)
            query = "SELECT 'time', latitude , longitude, place, mag FROM all_month WHERE mag = '" + str(magnitude_value) + "'"
            # print("inside else")
            memhash = hashlib.sha256(query.encode()).hexdigest()
            startTime = time.time()
            list_dict_Data = r.get(memhash)

            # print(list_dict_Data[0])
            # print(i)
            if not list_dict_Data:
                # print("from db")
                print('Not in cache')

                cursor.execute(query)
                list_dict_Data = cursor.fetchall()
                # print('list_dict_Data')
                # print(list_dict_Data)
                if x == 0:
                    # print("from db")
                    print('Hi first block')
                    list_dict_DataDisplay = list_dict_Data
                endTime = time.time()
                memData = []
                for row in list_dict_Data:
                    # print('row')
                    # print(row)
                    memDataDict = dict()
                    for i, val in enumerate(row):
                        # print('i')
                        # print(i)
                        # if type(val) == datetime:
                        #     val = time.mktime(val.timetuple())
                        memDataDict[columns[i]] = val
                    memData.append(memDataDict)
                r.set(memhash, dumps(memData))
                print('Hi')
                executionTime = (endTime - startTime) * 1000
                if x == 0:
                    print('Hi again')
                    firstExecutionTime = executionTime
                else:
                    print('Not 0 iteration')    
                totalExecutionTime = totalExecutionTime + executionTime
                

            else:
                print('In cache')
                list_dict_Data = loads(list_dict_Data.decode())
                if x == 0:
                    list_dict_DataDisplay = list_dict_Data
                endTime = time.time()
            executionTime = (endTime - startTime) * 1000
            if x == 0:
                    firstExecutionTime = executionTime
            totalExecutionTime = totalExecutionTime + executionTime
    conn.close()
    # print(list_dict_Data)
    return render_template('index.html', tableData=list_dict_DataDisplay, tableDataLen=list_dict_DataDisplay.__len__(), executionTime=totalExecutionTime, firstExecutionTime=firstExecutionTime)

# @app.route('/magsearch',methods=['GET', 'POST'])

# def magsearch():
#         limit = request.form['limit']
#         mag1 = request.form['magnitude1']
#         mag2 = request.form['magnitude2']
#         # mag = random.randrange(mag1,mag2,1)
#         cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:sagarserver3122.database.windows.net,1433;Database=sagar3122sql;Uid=sagar3122@sagarserver3122;Pwd=steve@3122HOLMES;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
#         cursor = cnxn.cursor()
#         for x in random.randrange(mag1,mag2,1)
#         cursor.execute("Select TOP "+limit+" * from all_month WHERE mag= '"+mag+"'")
#         result = cursor.fetchall()
#             print(result)
#         return render_template("magsearch.html", row=result)






port = os.getenv("PORT", 5000)

if __name__ == '__main__':
   app.run(debug="true",port=int(port))
    #  app.run("0.0.0.0",port=port)
