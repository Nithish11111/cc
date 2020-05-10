import flask
from flask import *
from flask import Flask
from flaskext.mysql import MySQL
import mysql.connector
import json
import requests
import re
import csv


app = Flask(__name__)
conn = mysql.connector.connect(
  host="db1",
  user="user",
  database="ride",
  password="password",
  port="3306"
)


cur =conn.cursor(buffered=True)
cur.execute("create table if not exists ride_info (ride_id int primary key auto_increment,created_by varchar(500),source varchar(500),destination varchar(500),timestamp varchar(500),riders varchar(6000))")
cur.execute("create table if not exists users (username varchar(600) primary key,password varchar(40))")

with open('AreaNameEnum.csv', mode='r') as infile:
    reader = csv.reader(infile)
    mydict = {rows[0]:rows[1] for rows in reader}

# Starting In Order to Code The API

#API No. 3 Create a New Ride
@app.route('/api/v1/rides', methods = ["POST"])
def create_ride():
    """
    201: Created
    400: Bad Request
    405: Method Not Allowed
    """
    created = flask.request.json["created_by"]
    result = requests.get(url="http://web:8080/api/v1/users")
    temp = [created,]
    t = json.loads(result.text)
    print(temp)
    print(temp in t)
    if (temp in t):
        timestamp = flask.request.json["timestamp"]
        source = flask.request.json["source"]
        destination = flask.request.json["destination"]
        source = mydict[source]
        destination = mydict[destination]
        
        write_data = "http://127.0.0.1:5000/api/v1/db/write"
        data = {"insert":[created,source,destination,timestamp],"columns":["created_by","source","destination","timestamp"],"table":"ride_info","type":"create"}
        try:
            r=requests.post(url = write_data, json = data)
            if (r.text == "success"):
                return jsonify({}),201
            else:
                return '',500
        except:
            return '',500
    else:
        return '',400

#API No. 4 List Rides Given Src and Destn
@app.route('/api/v1/rides' ,methods = ["GET"])

def list_rides():
    """
    200 : OK
    400 : Bad Request
    405 : Method Not Allowed
    """
    #print("request method is : ",flask.request.method)
    source = flask.request.args.get("source")
    destination = flask.request.args.get("destination")
    if (int(source) not in range(1,200) or int(destination) not in range(1,200)):
        return '',400
    source = mydict[source]
    destination = mydict[destination]
    read_data = {"table":"ride_info","type":"read","columns":"ride_id,created_by,timestamp","where":"source="+'\''+source+'\''+" and destination="+'\''+destination+'\''}
    read_api_end = "http://127.0.0.1:5000/api/v1/db/read"
    r=requests.post(url = read_api_end, json = read_data)
    resp = r.text
    print(resp)
    resp=json.loads(resp)
    if resp == []:
        return '',400
    reply = []
    temp_d = {}
    for i in resp:
        temp_d["ride_id"]=i[0]
        temp_d["username"]=i[1]
        temp_d["timestamp"]=i[2]
        reply.append(temp_d)
    return jsonify(reply),200

#API No.5 List Ride Details Given Ride ID

@app.route('/api/v1/rides/<rideId>',methods = ["GET"])
def ride_details(rideId):
    """
    200 : OK
    204 : No Content
    405 : Method Not Allowed
    """
    read_data = {"table":"ride_info","columns":"*","where":int(rideId),"type":"read"}
    read_api_end = "http://127.0.0.1:5000/api/v1/db/read"
    r=requests.post(url = read_api_end, json = read_data)
    resp = r.text
    resp=json.loads(resp)
    if resp == []:
       return '',204
    resp_list = resp[0]
    user_list = json.loads(resp_list[5])
    resp_dict = {}
    resp_dict["users"] = user_list
    resp_dict["ride_id"] = resp_list[0]
    resp_dict["Created_by"] = resp_list[1]
    resp_dict["Timestamp"] = resp_list[4]
    resp_dict["source"] = resp_list[2]
    resp_dict["destination"] = resp_list[3]
    return jsonify(resp_dict),200

#API No.6 To Join a Ride

@app.route('/api/v1/rides/<rideId>', methods = ["POST"])
def join_ride(rideId):
    """
    200 : OK
    400 : Bad Request
    405 : Method Not Allowed
    """
    username = flask.request.json["username"]
    result = requests.get(url="http://web:8080/api/v1/users")
    temp = [username,]
    t = json.loads(result.text)
    print(temp)
    print(temp in t)
    if (temp in t):
        read_data = {"table":"ride_info","columns":"*","where":int(rideId),"type":""}
        read_api_end = "http://127.0.0.1:5000/api/v1/db/read"
        r = requests.post(url = read_api_end, json = read_data)
        resp2 = r.text
        resp2 = json.loads(resp2)
        if resp2 == []:
           return '',400
        resp_list = resp2[0]
        user_list = json.loads(resp_list[5])
        user_list.append(username)
        user_list = json.dumps(user_list)
        #print(user_list,type(user_list))
        #print(resp)
        data = {"insert":user_list,"columns":"riders","table":"ride_info","type":"update","where":rideId}
        write_data = "http://127.0.0.1:5000/api/v1/db/write"
        try:
            r=requests.post(url = write_data, json = data)
            if (r.text == "success"):
                return jsonify({}),200
            else:
                return '',500
        except:
            return '',500

    else:
        return '',400

#API No. 7

@app.route('/api/v1/rides/<rideId>' , methods = ["DELETE"])
def delete_ride(rideId):
    """
    200 : OK 
    400 : Bad Request
    405 : Method Not Allowed
    """
    read_data={"insert":"","table":"ride_info","columns":"","where":"ride_id="+rideId,"type":"delete"}
    read_api_end = "http://127.0.0.1:5000/api/v1/db/read"
    r=requests.post(url = read_api_end, json = read_data)
    resp = r.text
    resp=json.loads(resp)
    #print(resp)
    if resp == []:
        return '',400
    write_data = "http://127.0.0.1:5000/api/v1/db/write"
    data = {"insert":rideId,"table":"ride_info","columns":"","where":"ride_id="+rideId,"type":"delete"}
    r=requests.post(url = write_data, json = data)
    return jsonify({}),200


#API No. 8

@app.route('/api/v1/db/write', methods = ["POST"])
def write_to_db():
    if (flask.request.method == "POST"):
        insert = flask.request.json["insert"]
        columns = flask.request.json["columns"]
        table = flask.request.json["table"]
        if (table == "users" and flask.request.json["type"] == "create"):
            cur.execute("INSERT INTO "+table+" ("+columns[0]+","+columns[1]+" )"+ " values (%s, %s)", (insert[0], insert[1]))
            conn.commit()
            #mysql.connection.close()
            return "success"

        elif (table == "users" and flask.request.json["type"] == "delete"):
            #print("DELETE FROM users WHERE username="+insert)
            cur.execute("DELETE FROM users WHERE username="+'\''+insert+'\'')
            conn.commit()
            return "success"

        elif (table == "ride_info" and flask.request.json["type"] == "update"):
            cur.execute("UPDATE ride_info SET riders="+'\''+insert+'\''+" WHERE ride_id="+str(flask.request.json["where"]))
            # conn.commit()
            #print("UPDATE ride_info SET riders="+'\''+insert+'\''+" WHERE ride_id="+str(flask.request.json["where"]))
            conn.commit()
            return "success"

        elif (table == "ride_info" and flask.request.json["type"] == "delete"):
            #print("DELETE FROM ride_info WHERE ride_id="+insert)
            cur.execute("DELETE FROM ride_info WHERE ride_id="+insert)
            conn.commit()
            return "success"

        elif (table == "ride_info" and flask.request.json["type"] == "create"):
            #print("type of columns ",type(columns),columns)
            #print("type of insert ",type(insert),insert)
            l=[]
            #l.append(insert[0])
            l=json.dumps(l)
            cur.execute("INSERT INTO "+table+" ("+columns[0]+","+columns[1]+","+columns[2]+","+columns[3]+",riders)"+ " values (%s, %s, %s, %s, %s)", (insert[0], insert[1], insert[2], insert[3],l))
            conn.commit()
            return "success"

    else:
        abort(405)

#API No.9

@app.route('/api/v1/db/read', methods = ["POST"])
def read_to_db():
    #print(flask.request.json)
    table = flask.request.json["table"]
    columns = flask.request.json["columns"]
    where = flask.request.json["where"]
    if table == "users" or (table == "ride_info" and flask.request.json["type"] == "delete"):
        #For API No 2 and 7
        where = where.split("=")
        #print(where)
        cur.execute("SELECT * FROM "+table+" where "+where[0]+"= '%s'", (where[1]))
        conn.commit()
        data = cur.fetchall()
        #print("type of data is ",type(data),"and data is ",data[0][0])
        return jsonify(data)

    elif table == "ride_info" and columns == '*':
        #For API NO.5
        # print("SELECT * FROM ride_info where ride_id="+str(where))
        cur.execute("SELECT * FROM ride_info where ride_id="+str(where))
        conn.commit()
        data = cur.fetchall()
        return jsonify(data)

    elif table == "ride_info":
        #print("select ride_id,created_by,timestamp from ride_info where "+where)
        cur.execute("select ride_id,created_by,timestamp from ride_info where "+where)
        conn.commit()
        data = cur.fetchall()
        return jsonify(data)

#API No. 11 Clear Database
@app.route('/api/v1/db/clear', methods = ["POST"])
def clear_db():
    if (flask.request.method == "POST"):
        cur.execute("SELECT * FROM users")
        conn.commit()
        data_users = cur.fetchall()
        cur.execute("SELECT * FROM ride_info")
        conn.commit()
        data_ride = cur.fetchall()
        if (data_ride):
            print("YES")
        else:
            print("NO")
        try:
            cur.execute("DELETE FROM ride_info")
            conn.commit()
            cur.execute("DELETE FROM users")
            conn.commit()
            return jsonify({}),200
        except:
            return '',500
    else:
        return '',405

if __name__ == '__main__':
    app.run(host="0.0.0.0",debug = True)
