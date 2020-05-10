import mysql.connector

mydb = mysql.connector.connect(
  host="db",
  user="user",
  password="password",
  database="ride"
)

mycursor = mydb.cursor()

mycursor.execute("create table ride_info (ride_id int primary key auto_increment,created_by varchar(500),source varchar(500),destination varchar(500),timestamp varchar(500),riders varchar(6000))")

mycursor.execute("create table users (username primary key varchar(600),password varchar(40))")
