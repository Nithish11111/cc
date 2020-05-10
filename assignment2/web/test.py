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
  host="db",
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

#API No.1 Add username and password
@app.route('/api/v1/users', methods = ["PUT"])
def add_user():
    """
    201: Created 
    400: Bad Request
    405: Method Not Allowed
    500: Internal Server Error
    """
    
    username = flask.request.json["username"]
    password = flask.request.json["password"]
    read_data={"table":"users","columns":"username","type":"","where":"username="+username}
    read_api_end = "http://127.0.0.1:5000/api/v1/db/read"
    r=requests.post(url = read_api_end, json = read_data)
    resp = r.text
    resp=json.loads(resp)
    print(resp,type(resp),resp != [])
    if resp != []:  #user already exists
        return '',400
    if not re.match("^[a-fA-F0-9]{40}$",password): #Password Not Hashed
        return '',400 
    api_end = "http://127.0.0.1:5000/api/v1/db/write"
    data = {"insert":[username,password],"columns":["username","password"],"table":"users","type":"create"}
    try:
        r=requests.post(url = api_end, json = data)
        return jsonify({}),201
    except:
        return '',500

#API No.2 Delete an existing user
@app.route('/api/v1/users/<username>', methods = ["DELETE"])
def delete_user(username):
    """
    200 : OK 
    400 : Bad Request
    405 : Method Not Allowed
    """
    #print("Username to delete: ",username)
    read_data={"table":"users","columns":"username","where":"username="+username}
    read_api_end = "http://127.0.0.1:5000/api/v1/db/read"
    r=requests.post(url = read_api_end, json = read_data)
    resp = r.text
    resp=json.loads(resp)
    print(resp)
    if resp != []:
        write_data = "http://127.0.0.1:5000/api/v1/db/write"
        data = {"insert":username,"columns":"username","table":"users","type":"delete"}
        r=requests.post(url = write_data, json = data)
        #cur.execute("DELETE FROM users WHERE username = %s",username)
        #conn.commit()
        return jsonify({}),200 #User deleted Successfully
    return '',400 #User Not Present

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

#API No. 10 List all users
@app.route('/api/v1/users', methods = ["GET"])
def list_users():
    #print(flask.request.json)
    if(flask.request.method == "GET"):
        table = "users"
        cur.execute("SELECT username FROM "+table)
        conn.commit()
        data = cur.fetchall()
        print(data)
        data = jsonify(data)
        #res = jsonify(data
        #print(json.loads(data))
        if (data == ''):
            return '',204
        else:
            return data,200
    else:
        return '',405

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
