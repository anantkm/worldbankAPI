#COMP_9321_Data_Services_Engineering.
#Assignment2 
#Authour: Anant Krishna Mahale (z5277610)


#________________________________________________________________________________________________________________________
#All Library imports. 
import pandas as pd
from flask import Flask
from flask_restplus import Resource, Api, fields
import sqlite3
import os
import json
import datetime
import re
from flask import request
from flask_restplus import reqparse
import requests
#________________________________________________________________________________________________________________________

app = Flask(__name__)
api = Api(app, 
            default="World Bank Indicators",            
            title="World Bank Indicators", 
            description="Assignment_2_COMP9321\n\nAPI for World Bank Economic Indicators. \n\nAuthor: Anant Krishna Mahale\n\nzID:5277610")

#________________________________________________________________________________________________________________________



#SQL related functions. 
# A function to execute SQL command
def dbQuery_handler(database, command):
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    cursor.execute(command)         #retreive and return from the database.
    result = cursor.fetchall()      
    connection.commit()
    connection.close()
    return result

#Function to update the collection Table. 
def updateCollectionTable(database, given_id, data):
    curDateTime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    collection = "INSERT INTO Collection VALUES ({}, '{}', '{}', '{}');"\
        .format(given_id, data[0]['indicator']['id'], data[0]['indicator']['value'],curDateTime)
    #print(collection)
    dbQuery_handler(database, collection)

#Function to update the Entries table. 
def updateEntriesTable(database, given_id, data):
    entry = "INSERT INTO Entries VALUES"
    for sub_data in data:
        if (sub_data['value'] != None):
            date = re.sub('\D', '', sub_data['date'])
            country = sub_data['country']['value']
            country = country.replace("'", "''")
            entry += f"({given_id}, '{country}', '{date}', '{sub_data['value']}'),"
    entry = entry.rstrip(',') + ';'
    if len(entry)>len("INSERT INTO Entries VALUES;"):
        dbQuery_handler(database, entry)


# function to create a database if it does not exists. 
def create_database(db_file):
    #ref: https://www.tutorialspoint.com/sqlite/sqlite_python.htm 
    if os.path.exists(db_file):
        print('Database already exists.')
        return False
    print('Creating database ...')
    connection = sqlite3.connect(db_file)

    print ('Opened database successfully')
    
    connection.execute('''CREATE TABLE Collection(
                    id INTEGER UNIQUE NOT NULL,
                    indicator VARCHAR(100),
                    indicator_value VARCHAR(100),
                    creation_time DATE,
                    CONSTRAINT id_pkey PRIMARY KEY (id));''')

    print ("Collection created successfully")
    connection.execute('''CREATE TABLE Entries(
                    id INTEGER NOT NULL,
                    country VARCHAR(100),
                    date VARCHAR(100),
                    value VARCHAR(100),
                    CONSTRAINT entry_fkey FOREIGN KEY (id) REFERENCES Collection(id));''')

    print ("Entries created successfully")
    connection.close()
    return True



#________________________________________________________________________________________________________________________


# Function that makes the remote call to fetch data from worldbank API
def remote_request(indicator, page, start=2012, end=2017, content_format='json'):
    url = f'http://api.worldbank.org/v2/countries/all/indicators/' + \
          f'{indicator}?date={start}:{end}&format={content_format}&per_page=500&page={page}'
    response = requests.get(url)
    data = response.text
    if re.findall('Invalid value', str(data), flags=re.I):
        return False,False
    elif(page>1):
        return json.loads(data)[1]
    else:
        return json.loads(data)[0]['pages'], json.loads(data)[1]

#________________________________________________________________________________________________________________________

# functions to format the response for different questions. 

def sucussResponseFunction_q1_q3(query_result):
    return {"uri": "/collection/{}".format(query_result[0]),
            "id": int(query_result[0]),
            "creation_time": "{}".format(query_result[3]),
            "indicator_id": "{}".format(query_result[1])
            }

def sucussResponseFunction_q4_q6(collection_query, entries_query):
    result = {"id": int(collection_query[0]),
              "indicator": "{}".format(collection_query[1]),
              "indicator_value": "{}".format(collection_query[2]),
              "creation_time": "{}".format(collection_query[3]),
              "entries": []
              }
    for i in range(len(entries_query)):
        result["entries"].append({"country": entries_query[i][0],
                                  "date": entries_query[i][1],
                                  "value": entries_query[i][2]
                                  })
    return result

def sucussResponseFunction_q5 (join_query):
    return {
        "id": "{}".format(join_query[0][0]),
        "indicator": "{}".format(join_query[0][1]),
        "country": "{}".format(join_query[0][2]),
        "year": "{}".format(join_query[0][3]),
        "value": "{}".format(join_query[0][4])
        }, 200



#________________________________________________________________________________________________________________________
         

#helper fucntions:

# function to handle all the get requests. [Question: 3,4,5,6]
inputparams_q3 = ['+id','-id','+creation_time','-creation_time','+indicator','-indicator']  #params to check for the question_3

def getHandler(database, action, **kwargs):
    
    #question 3, get all collections info
    if action == 'getall':
        sort_by = kwargs['sort_by']
        check_plus_id = 0
        check_min_id = 0
        check_plus_ct =0
        check_min_ct = 0
        check_plus_indctr = 0
        check_min_indctr = 0

        for i in range(len(sort_by)):
            temp = inputparams_q3.index(sort_by[i])
            if temp == 0:
                check_plus_id=1
            elif temp == 1:
                check_min_id =1             
            elif temp == 2:
                check_plus_ct = 1
            elif temp == 3:
                check_min_ct = 1              
            elif temp == 4:
                check_plus_indctr = 1                
            elif temp == 5:
                check_min_indctr = 1                

        connection = sqlite3.connect('z5277610.db')
        query = pd.read_sql_query('''SELECT * FROM Collection''', connection )
        if not query.empty:

            if check_plus_indctr:
                query.sort_values(by='indicator',ascending=True, inplace=True)
            if check_min_indctr:
                query.sort_values(by='indicator',ascending=False, inplace=True)
            if check_plus_ct: 
                query.sort_values(by='creation_time',ascending=True, inplace=True)
            if check_min_ct:
                query.sort_values(by='creation_time',ascending=False, inplace=True)
            if check_plus_id:
                query.sort_values(by='id',ascending=True, inplace=True)
            if check_min_id:
                query.sort_values(by='id',ascending=False, inplace=True)


        if not query.empty:
            result_list = list()
            for i in range(len(query)):
                result_list.append(sucussResponseFunction_q1_q3(query.iloc[i]))
            return result_list, 200
        return {"message": f"No collections not found in database!"}, 404

    # question 4, get one specified collection and its data
    elif action == 'getone':
        collection_query = dbQuery_handler(database,
                                               f"SELECT * "
                                               f"FROM Collection "
                                               f"WHERE id = {kwargs['id']};")

        entries_query = dbQuery_handler(database,
                                            f"SELECT country, date, value "
                                            f"FROM Entries "
                                            f"WHERE id = {kwargs['id']};")
        if collection_query:
            return sucussResponseFunction_q4_q6(collection_query[0], entries_query), 200
        return {"message":
                f"The id {kwargs['id']} not found in database!"}, 404
    
    # question 5 Retrieve economic indicator value for given country and a year 
    elif action == 'getecoind':
        join_query = dbQuery_handler(database,
                                         f"SELECT c.id, c.indicator, country, date, value "
                                         f"FROM Collection c "
                                         f"JOIN Entries ON (c.id = Entries.id) "
                                         f"WHERE c.id = {kwargs['id']} "
                                         f"AND date = '{kwargs['year']}' "
                                         f"AND country = '{kwargs['country']}';")
        if join_query:
            return sucussResponseFunction_q5(join_query)
        return {"message":
                f"The Collection with given arguments {kwargs} not found in database!"}, 404

    # question 6, get data for specified year, id, sort by its value, can be either descent or ascent.
    elif action == 'gettopbottom':
        order_flag = ''
        if kwargs['flag'] == 'top':     # if get top, it should be reverse sort and limit first values
            order_flag = 'DESC'

        collection_query = dbQuery_handler(database,
                                               f"SELECT * FROM Collection WHERE id = {kwargs['id']};")

        # should use cast(value as real), otherwise it sorted by string order
        if kwargs['value'] != 0:
            entries_query = dbQuery_handler(database,
                                            f"SELECT country, date, value "
                                            f"FROM Entries "
                                            f"WHERE id = {kwargs['id']} "
                                            f"AND date = '{kwargs['year']}' "
                                            f"GROUP BY country, date, value "
                                            f"ORDER BY CAST(value AS REAL) {order_flag} "
                                            f"LIMIT {kwargs['value']};")
        else:
            entries_query = dbQuery_handler(database,
                                            f"SELECT country, date, value "
                                            f"FROM Entries "
                                            f"WHERE id = {kwargs['id']} "
                                            f"AND date = '{kwargs['year']}' "
                                            f"GROUP BY country, date, value "
                                            f"ORDER BY CAST(value AS REAL) {order_flag};")
        if collection_query:
            result_dict = sucussResponseFunction_q4_q6(collection_query[0], entries_query)
            result_dict.pop("id")
            result_dict.pop("creation_time")
            return result_dict, 200
        return {"message":
                f"No data matches your specified arguments in the database!"}, 404    
#________________________________________________________________________________________________________________________

def postHandler(database, indicator):
    query = None
    try:
        query = dbQuery_handler(database, f"SELECT * FROM Collection WHERE indicator = '{indicator}';")
    except:
        pass
    if query:
        return sucussResponseFunction_q1_q3(query[0]), 200
    else:
        total_pages, data_download = remote_request(indicator,1)
        if not data_download:
            return {"message": f"The indicator '{indicator}' is not valid. Please provide valid Indicator!"}, 404
        new_id = re.findall('\d+', str(dbQuery_handler(database, 'SELECT MAX(id) FROM Collection;')))
        if not new_id:
            new_id = 1
        else:
            new_id = int(new_id[0]) + 1
        updateCollectionTable(database, new_id, data_download)
        updateEntriesTable(database, new_id, data_download)
        for i in range(2,total_pages+1):
            data_remaining = remote_request(indicator, i)
            if data_remaining:
                updateEntriesTable(database, new_id, data_remaining)        
        new_query = dbQuery_handler(database, f"SELECT * FROM Collection WHERE indicator = '{indicator}';")
        return sucussResponseFunction_q1_q3(new_query[0]), 201
    
#________________________________________________________________________________________________________________________

#delete requests, for question 2
def deleteHandler(database, id):
    query = dbQuery_handler(database,
                                f"SELECT * FROM Collection WHERE id = {id};"
                                )
    if not query:
        return {"message": f"Collection with id:{id} NOT FOUND in the database!"}, 404
    else:
        dbQuery_handler(database, f"DELETE FROM Entries WHERE id = {id};")
        dbQuery_handler(database, f"DELETE FROM Collection WHERE id = {id};")
        return {
                "message": f"The collection {id} was removed from the database!",
                "id": id
                }, 200

#________________________________________________________________________________________________________________________


# this function calls respective function based on the action. 

def processRequest(database, action, **kwargs):
    if action == 'post':
        return postHandler(database, kwargs['indicator_id'])
    
    elif action == 'delete':
        return deleteHandler(database,kwargs['id']) 
    
    elif action == 'getone':
        return getHandler(database,'getone', id=kwargs['id'])
    
    elif action == 'getecoind':
        return getHandler(database, 'getecoind', id=kwargs['id'],
                           year=kwargs['year'], country=kwargs['country'])

    elif action == 'getall':
        return getHandler(database, 'getall', sort_by =kwargs['sort_by'])

    elif action == 'gettopbottom':
        if kwargs['query'] == None:
            return getHandler(database, 'gettopbottom', id=kwargs['id'],
                               year=kwargs['year'], flag='top', value=0)
        check_plus = re.search("^([+])(\d+)$", kwargs['query'])
        check_minus = re.search("^([-])(\d+)$", kwargs['query'])
        if check_plus:
            return getHandler(database, 'gettopbottom', id=kwargs['id'],
                               year=kwargs['year'], flag='top', value=check_plus.group(2))
        if check_minus:
            return getHandler(database, 'gettopbottom', id=kwargs['id'],
                               year=kwargs['year'], flag='bottom', value=check_minus.group(2))
        else:
            return {"message":
                    "Your input arguments are not in correct format! Must be either +<int> or -<int>."}, 400
#________________________________________________________________________________________________________________________


parser = reqparse.RequestParser()    #parser for q1
parser.add_argument('indicator_id')  

parser1  = reqparse.RequestParser()  #parser for q6
parser1.add_argument('q', type=str, help='Write your query here(e.g."+10")', location='args')

parser2  = reqparse.RequestParser()  #parser for q3
parser2.add_argument('order_by', type=str, help='Your query here (e.g."{+id,+creation_time}")', location='args')

 
# single query parameter route class, for question 1 and 3 
@api.route('/collections')
@api.response(200, 'OK Success')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
@api.response(201, 'Created')

class SingleQueryClass(Resource):
    @api.doc(parser  = parser)
    @api.doc(description='Question-1: Import a collection from the data service')
    def post(self):
        args = parser.parse_args()        
        indicator_id = args.get('indicator_id')  # retrieve the query parameters
        if not indicator_id :
            return {
                "message": "Please check if the indicator_id is given!"
            }, 400
        indicator_id = indicator_id.strip()
        return processRequest('z5277610.db', 'post', indicator_id = indicator_id)
    
    @api.doc(parser  = parser2)
    @api.doc(description='Question 3 - Retrieve the list of available collections')
    def get(self):                
        args2 = parser2.parse_args()        
        params = args2.get('order_by')  # retrieve the query parameters 
        if params:
            count_id = params.count("id")
            count_ct = params.count("creation_time")
            count_indictr = params.count("indicator")
            paramsList = list(params.split(","))
            if count_id <= 1 and count_ct<= 1 and count_indictr <=1 and (len(paramsList)<=3):
                for i in range(len(paramsList)):
                    if paramsList[i] not in inputparams_q3:
                        return {"message": f"Please provide valid parameters!"}, 400 
                return processRequest('z5277610.db', 'getall', sort_by =paramsList ) 
            else:
                return {"message": f"Please provide valid parameters!"}, 400 

        else:
            return processRequest('z5277610.db', 'getall',sort_by =['+id'])


   

# path parameter route class for question 2 and 4
@api.route("/collections/<int:id>")
@api.response(200, 'OK Success')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
class SingleParamClass(Resource):
    
    @api.doc(description='Question 2- Deleting a collection with the data service')
    def delete(self, id):
        return processRequest('z5277610.db', 'delete', id=id)
    
    @api.doc(description='Question 4 - Retrieve a collection')
    def get(self, id):
        return processRequest('z5277610.db', 'getone', id=id)


# 3 parameter route class, for question 5
@api.route("/collections/<int:id>/<int:year>/<string:country>")
@api.response(200, 'OK Success')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
@api.doc(description='Question 5 - Retrieve economic indicator value for given country and a year')
class ThreeParamClass(Resource):
    
    def get(self, id, year, country):
        return processRequest('z5277610.db', 'getecoind', id=id,
                               year=year, country=country)


#route class for question 6 combination of query and 2 parameter

@api.route("/collections/<int:id>/<int:year>")
@api.response(200, 'OK Success')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
@api.doc(parser = parser1)
@api.doc(description = 'Question 6 - Retrieve top/bottom economic indicator values for a given year')
class TwoParamClass(Resource):
    
    def get(self, id, year):
        args1 = parser1.parse_args()        
        params = args1.get('q')  # retrieve the query parameters  
        return processRequest('z5277610.db', 'gettopbottom', id=id,
                               year=year, query=params)


if __name__ == '__main__':
    create_database('z5277610.db')
    app.run()