import sys
import psycopg2
import pyodbc
import pandas as pd
import configparser
import os
from datetime import datetime
from sqlalchemy import create_engine
import requests
import json
# sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
# os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')

# config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
# config=configparser.ConfigParser(allow_no_value=True)
# config.read(config_file)

# mssqluid = os.path.join(os.path.dirname("__file__"),config['develop']['mssqluid'])
# mssqlserver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
# msqsldriver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
# mssqldwhdb = os.path.join(os.path.dirname("__file__"),config['develop']['mssqldb'])
# pgpw=os.path.join(os.path.dirname("__file__"),config['develop']['pgpw'])
# pguid=os.path.join(os.path.dirname("__file__"),config['develop']['pguid'])
# pgserver=os.path.join(os.path.dirname("__file__"),config['develop']['pgserver'])
# pgdwhdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgdwhdb'])
# pgport=os.path.join(os.path.dirname("__file__"),config['develop']['pgport'])
# path_validator=os.path.join(os.path.dirname("__file__"),config['develop']['path_validator'])
# pg_conn_str=os.path.join(os.path.dirname("__file__"),config['develop']['pg_conn_str'])


def connect_to_warehouse():
    # get db connection parameters from the conf file
    parser = configparser.ConfigParser()
    parser.read("../Config/config.ini")
    dbname = parser.get("develop", "pgdwhdb")
    user = parser.get("develop", "pguid")
    password = parser.get("develop", "pgpw")
    host = parser.get("develop", "pgserver")
    port = parser.get("develop", "pgport")
    rs_conn = psycopg2.connect(
    "dbname=" + dbname
    + " user=" + user
    + " password=" + password
    + " host=" + host
    + " port=" + port)
    return rs_conn


def execute_test(
    db_conn,
    script_1,
    script_2,
    comp_operator):
    # execute the 1st script and store the result
    cursor = db_conn.cursor()
    sql_file = open(script_1, 'r')
    cursor.execute(sql_file.read())
    record = cursor.fetchone()
    result_1 = record[0]
    db_conn.commit()
    cursor.close()
    # execute the 2nd script and store the result
    cursor = db_conn.cursor()
    sql_file = open(script_2, 'r')
    cursor.execute(sql_file.read())
    
    record = cursor.fetchone()
    result_2 = record[0]
    db_conn.commit()
    cursor.close()
    print("result 1 = " + str(result_1))
    print("result 2 = " + str(result_2))
    # compare values based on the comp_operator
    if comp_operator == "equals":
        return result_1 == result_2
    elif comp_operator == "greater_equals":
        return result_1 >= result_2
    elif comp_operator == "greater":
        return result_1 > result_2
    elif comp_operator == "less_equals":
        return result_1 <= result_2
    elif comp_operator == "less":
        return result_1 < result_2
    elif comp_operator == "not_equal":
        return result_1 != result_2
    # if we made it here, something went wrong
    return False

parser = configparser.ConfigParser()
parser.read("../Config/config.ini")
webhook_url = parser.get("develop", "webhook_url")
# test_result should be True/False
def send_slack_notification(
    webhook_url,
    script_1,
    script_2,
    comp_operator,
    test_result):
    try:
        if test_result == True:
            message = ("Validation Test Passed!: " 
                       + script_1 + " / " 
                       + script_2 + " / " 
                       + comp_operator)
        else: 
            message = ("Validation Test FAILED!: " 
                       + script_1 + " / " 
                       + script_2 + " / " 
                       + comp_operator) 
            slack_data = {'text': message} 
            response = requests.post(webhook_url, 
                                     data=json.dumps(slack_data),
                                     headers={ 
                                         'Content-Type': 'application/json'
                                     })
        if response.status_code != 200: 
            print(response)
            return False
    except Exception as e:
        print("error sending slack notification")
        print(str(e))
        return False


if __name__ == "__main__": 
    if len(sys.argv) == 2 and sys.argv[1] == "-h": 
        print("Usage: python validator.py" 
              + "script1.sql script2.sql " 
              + "comparison_operator") 
        print("Valid comparison_operator values:")
        print("equals")
        print("greater_equals")
        print("greater")
        print("less_equals")
        print("less")
        print("not_equal")
        
        exit(0)
        
    if len(sys.argv) != 5:
        print("Usage: python validator.py" 
              + "script1.sql script2.sql " 
              + "comparison_operator")
        
        exit(-1)
        
    script_1 = sys.argv[1]
    script_2 = sys.argv[2]
    comp_operator = sys.argv[3]
    sev_level = sys.argv[4]
    
    # connect to the data warehouse
    db_conn = connect_to_warehouse()
    # execute the validation test
    test_result = execute_test(
        db_conn,
        script_1,
        script_2,
        comp_operator)
    
    print("Result of test: " + str(test_result))
    
    if test_result == True:
        exit(0)
    else:
        send_slack_notification(
            webhook_url,
            script_1,
            script_2,
            comp_operator,
            test_result)
    if sev_level == "halt":
        exit(-1)
    else:
        exit(0)