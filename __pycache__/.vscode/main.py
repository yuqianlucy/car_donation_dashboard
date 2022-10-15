#step 1: getting all the importing function
from __future__ import print_function
from dataclasses import field
#import mysql
import sched
from auth import spreadsheet_service
from auth import drive_service
import datetime
import pytz
import pandas as pd
import numpy as np
import gspread
import df2gspread as d2g
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import mysql.connector
import re
import sys
from time import time, sleep
import schedule
import os
import config 


#step 2: Get all the function defined
#defining the time function
def time():
    import datetime
    datetime.datetime.now().timestamp()
    retrievalDate = datetime.datetime.now(pytz.timezone('America/Los_Angeles'))
    
    return datetime.datetime.strftime(retrievalDate , "%Y-%m-%dT%H:%M:%SZ")

def create():
    spreadsheet_details = {
        'properties': {
            'title': 'LOH_Pending_Users_Info'
        }
    }
    sheet = spreadsheet_service.spreadsheets().create(body=spreadsheet_details, fields='spreadsheetId').execute()
    sheetId = sheet.get('spreadsheetId')
    # print('Spreadsheet ID: {0}'.format(sheetId))
    email = input("Please enter your email address: ")
    permission1 = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': email
    }
    drive_service.permissions().create(fileId=sheetId, body=permission1).execute()
    # print(sheetId)
    url = 'https://docs.google.com/spreadsheets/d/' + sheetId + '/edit#gid=0'
    print("A new spreadsheet has been created for you. Here's the link: {0}".format( url ))
    return sheetId

def get_colName():
    field_names = [i[0] for i in mycursor.description]
    column = []
    column.append(field_names)
    

    return column

def write_colName():
    spreadsheet_id = spreadsheet_Id
    values = get_colName()
    value_input_option = 'USER_ENTERED'
    body = {
        'values': values
    }
    
    result = spreadsheet_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption=value_input_option, body=body).execute()

def clear():
    # if sheet_cell == 'Y':
    #     sheetName = 'Raw data'
    # elif sheet_cell == 'N':
    #     sheetName = str(sheet_name)
    sheetName=sheet_name
    body = {}
    resultClear = spreadsheet_service.spreadsheets().values().clear( spreadsheetId=spreadsheet_Id, range=sheetName,
                                                       body=body ).execute( )
    print('{0} cleared.'.format( sheetName ))


def export_pandas_df_to_sheets(spreadsheet_id, df):
    # if new_gs == 'N':
    #     sclear = input("Do you want to clear the sheet? Y or N ").upper()
    #     if sclear == 'Y':
    #         clear()
    #     elif sclear == 'N':
    #         pass
    # clear()
     
    column_names=get_colName()
    write_colName()

    df.replace(np.nan, "", inplace=True)
    df.replace('NaT', "", inplace=True)
    body = {
        'values': df.values.tolist()
    }
    
    result = spreadsheet_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, body=body, valueInputOption='USER_ENTERED', range=data_range).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))
def updateSheetFormat1(sheetId):
    DATAS= {
        "requests": [
            {
            "repeatCell": {
                "range": {
                "sheetId": sheetId,
                "startRowIndex": 1,
                # "endRowIndex": 10,
                "startColumnIndex": 5,
                "endColumnIndex": 6
                },
                "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                    "type": "NUMBER",
                    "pattern": "[$$-411]#,##0.00"
                    }
                }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
            }
        ]
        }
        
    result = spreadsheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_Id, body=DATAS).execute()
def updateSheetFormat1(sheetId):
    DATAS1= {
        "requests": [
            {
            "repeatCell": {
                "range": {
                "sheetId": sheetId,
                "startRowIndex": 1,
                # "endRowIndex": 10,
                "startColumnIndex": 6,
                "endColumnIndex": 7
                },
                "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                    "type": "NUMBER",
                    "pattern": "[$$-411]#,##0.00"
                    }
                }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
            }
        ]
        }
        
    result = spreadsheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_Id, body=DATAS1).execute()

def updateSheetFormat1(sheetId):
    DATAS= {
        "requests": [
            {
            "repeatCell": {
                "range": {
                "sheetId": sheetId,
                "startRowIndex": 1,
                # "endRowIndex": 10,
                "startColumnIndex": 7,
                "endColumnIndex": 8
                },
                "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                    "type": "NUMBER",
                    "pattern": "[$$-411]#,##0.00"
                    }
                }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
            }
        ]
        }
        
    result = spreadsheet_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_Id, body=DATAS).execute()   
def sqlQuering():
    
    mydb = mysql.connector.connect(
        host=os.getenv("HOST"),
        user=os.getenv("CDUSER"),
        password=os.getenv("PASSWORD"),
        database=""
        )
    print("Connected!")
        
    global mycursor
    mycursor = mydb.cursor()

    # sql1 = """ select 'SF' operation, id, status, year (dateofsale) Sold_Year,month (dateofsale) Sold_month,  concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))as Sold_Yearmonth, date Donation_date, time, name, year car_year, make, model, mileage, plate, vin, phone, address, addr, email, infofrom, remarks, whopickup, cleanorsalvage, carfaxinfoowners, registrationfee, cost, workhours, whorepair, fixrecord, onlinedate, firstprice, dateofsale, CAST(price AS SIGNED) AS 'price' , null as 'Direct cost', null as 'cost type', null as 'Cost subtype', buyer, addrofbuyer, whosale, who1098, leavemessage, todolist, whotodo, salemileage, postdateofbill, donorinfo, complain, ticketNo, ticketdeadline, ticketSender, ticketAddress, stockworklog, areacode, isreferral, referrer, referralfee, buyerphone, buyerID, buyerEmail, plateNew, inventoryDate, categoryColor, mechanicalIssues, retailWholesaleJunk, newPlateFromDealer, vinAudit, kbb, dmvMarketValue, useTax, buyItNowPrice, specialPrice,NOW()
    # as data_refresh_time
    # FROM autolog.autolog 
    # union all 
    # select 'LA', id, status, year (dateofsale) Sold_year, month (dateofsale) Sold_month, concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))as Sold_Yearmonth, date Donation_date, time, name, year car_year, make, model, mileage, plate, vin, phone, address, addr, email, infofrom, remarks, whopickup, cleanorsalvage, carfaxinfoowners, registrationfee, cost, workhours, whorepair, fixrecord, onlinedate, firstprice, dateofsale, CAST(price AS SIGNED) AS 'price' , null as 'Direct cost', null as 'cost type', null as 'Cost subtype',buyer, addrofbuyer, whosale, who1098, leavemessage, todolist, whotodo, salemileage, postdateofbill, donorinfo, complain, ticketNo, ticketdeadline, ticketSender, ticketAddress, stockworklog, areacode, isreferral, referrer, referralfee, buyerphone, buyerID, buyerEmail, plateNew, inventoryDate, categoryColor, mechanicalIssues, retailWholesaleJunk, newPlateFromDealer, vinAudit, kbb, dmvMarketValue, useTax, buyItNowPrice, specialPrice,NOW()
    # as data_refresh_time
    # FROM autolog_la.autolog 
    # union all 
    # select 'ETC', id, status, year (dateofsale) Sold_year, month (dateofsale) Sold_month,  concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0')) as Sold_Yearmonth, date Donation_date,  time, name, year car_year, make, model, mileage, plate, vin, phone, address, addr, email, infofrom, remarks, whopickup, cleanorsalvage, carfaxinfoowners, registrationfee, cost, workhours, whorepair, fixrecord, onlinedate, firstprice, dateofsale, CAST(price AS SIGNED) AS 'price',null as 'Direct cost', null as 'cost type', null as 'Cost subtype', buyer, addrofbuyer, whosale, who1098, leavemessage, todolist, whotodo, salemileage, postdateofbill, donorinfo, complain, ticketNo, ticketdeadline, ticketSender, ticketAddress, stockworklog, areacode, isreferral, referrer, referralfee, buyerphone, buyerID, buyerEmail, plateNew, inventoryDate, categoryColor, mechanicalIssues, retailWholesaleJunk, newPlateFromDealer, vinAudit, kbb, dmvMarketValue, useTax, buyItNowPrice, specialPrice,NOW()
    # as data_refresh_time
    # FROM autolog_etc.autolog 
    # union all 
    # select 'Korea', id, status, year (dateofsale) Sold_year,month (dateofsale) Sold_month,  concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0')) as Sold_Yearmonth, date Donation_date,  time, name, year car_year, make, model, mileage, plate, vin, phone, address, addr, email, infofrom, remarks, whopickup, cleanorsalvage, carfaxinfoowners, registrationfee, cost, workhours, whorepair, fixrecord, onlinedate, firstprice, dateofsale, CAST(price AS SIGNED) AS 'price',null as 'Direct cost', null as 'cost type', null as 'Cost subtype', buyer, addrofbuyer, whosale, who1098, leavemessage, todolist, whotodo, salemileage, postdateofbill, donorinfo, complain, ticketNo, ticketdeadline, ticketSender, ticketAddress, stockworklog, areacode, isreferral, referrer, referralfee, buyerphone, buyerID, buyerEmail, plateNew, inventoryDate, categoryColor, mechanicalIssues, retailWholesaleJunk, newPlateFromDealer, vinAudit, kbb, dmvMarketValue, useTax, buyItNowPrice, specialPrice,NOW()
    # as data_refresh_time
    # FROM autolog_korea.autolog;"""
    sql2="""select operation, Year, Month, YearMonth, data_refresh_time,
    sum(revenue) AS revenue, sum(direct_cost) as direct_cost,  case when sum(direct_cost) is not null then (sum(revenue) -
    sum(direct_cost))  when sum(direct_cost) is null then sum(revenue)
    else
    null   end as profit,sum(number_sold) as number_sold, sum(number_donated) as number_donated
    from
    (
    select 'SF' operation, year (dateofsale) Year,month (dateofsale) Month, 
    concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))as YearMonth,
    convert(sum(CAST(price AS SIGNED)),CHAR) AS 'revenue', null as
    direct_cost, count(dateofsale) as number_sold,null number_donated,NOW()
    as data_refresh_time
    FROM autolog.autolog
    group by  year (dateofsale),month (dateofsale) ,
    concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))
    union all
    select 'SF' operation, year (date) Year,month (date) Month,
    concat(year(date),'-',LPAD(month (date),2,'0'))as YearMonth, null AS
    'revenue', null as direct_cost, null number_sold, count(date) as
    number_donated,NOW() as data_refresh_time
    FROM autolog.autolog
    group by year (date) ,month (date) , concat(year(date),'-',LPAD(month
    (date),2,'0'))
    union all
    select 'LA' operation, year (dateofsale) Year,month (dateofsale) Month, 
    concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))as YearMonth,
    convert(sum(CAST(price AS SIGNED)),CHAR) AS 'revenue', null as
    direct_cost, count(dateofsale) as number_sold,null number_donated,NOW()
    as data_refresh_time
    FROM autolog_la.autolog
    group by  year (dateofsale),month (dateofsale) ,
    concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))
    union all
    select 'LA' operation, year (date) Year,month (date) Month,
    concat(year(date),'-',LPAD(month (date),2,'0'))as YearMonth, null AS
    'revenue', null as direct_cost, null number_sold, count(date) as
    number_donated,NOW() as data_refresh_time
    FROM autolog_la.autolog
    group by year (date) ,month (date) , concat(year(date),'-',LPAD(month
    (date),2,'0'))
    union all
    select 'ETC' operation, year (dateofsale) Sold_Year,month (dateofsale)
    Sold_month,  concat(year(dateofsale),'-',LPAD(month
    (dateofsale),2,'0'))as Sold_Yearmonth, convert(sum(CAST(price AS
    SIGNED)),CHAR) AS 'revenue', null as direct_cost, count(dateofsale) as
    number_sold,null number_donated,NOW() as data_refresh_time
    FROM autolog_etc.autolog
    group by  year (dateofsale),month (dateofsale) ,
    concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))
    union all
    select 'ETC' operation, year (date) Sold_Year,month (date) Sold_month, 
    concat(year(date),'-',LPAD(month (date),2,'0'))as Sold_Yearmonth, null
    AS 'revenue', null as direct_cost, null number_sold, count(date) as
    number_donated,NOW() as data_refresh_time
    FROM autolog_etc.autolog
    group by year (date) ,month (date) , concat(year(date),'-',LPAD(month
    (date),2,'0'))
    union all
    select 'korea' operation, year (dateofsale) Sold_Year,month (dateofsale)
    Sold_month,  concat(year(dateofsale),'-',LPAD(month
    (dateofsale),2,'0'))as Sold_Yearmonth, convert(sum(CAST(price AS
    SIGNED)),CHAR) AS 'revenue', null as direct_cost, count(dateofsale) as
    number_sold,null number_donated,NOW() as data_refresh_time
    FROM autolog_korea.autolog
    group by  year (dateofsale),month (dateofsale) ,
    concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))
    union all
    select 'korea' operation, year (date) Sold_Year,month (date)
    Sold_month,  concat(year(date),'-',LPAD(month (date),2,'0'))as
    Sold_Yearmonth, null AS 'revenue', null as direct_cost, null
    number_sold, count(date) as number_donated,NOW() as data_refresh_time
    FROM autolog_korea.autolog
    group by year (date) ,month (date) , concat(year(date),'-',LPAD(month
    (date),2,'0'))
    union all
    select operations, year (service_date) Year, month (service_date)
    Month,  concat(year(service_date),'-',LPAD(month (service_date),2,'0'))
    as YearMonth,
    null AS 'revenue', sum(amounts) as direct_cost, count(service_date) as
    number_sold, null number_donated, NOW() as data_refresh_time
    FROM autolog.direct_cost
    group by operations, year (service_date), month (service_date),
    concat(year(service_date),'-',LPAD(month (service_date),2,'0'))
    ) as CTE_ALL_DATA
    group by
    operation, Year, Month, YearMonth, data_refresh_time;"""
    


    # sql2="""select operation, Year, Month, YearMonth, data_refresh_time,
    # sum(revenue) AS revenue, sum(direct_cost) as direct_cost,  case when sum(direct_cost) is not null then (sum(revenue) -
    # sum(direct_cost))  when sum(direct_cost) is null then sum(revenue)
    # else
    # null   end as profit,sum(number_sold) as number_sold, sum(number_donated) as number_donated
    # from
    # (
    # select 'SF' operation, year (dateofsale) Year,month (dateofsale) Month, 
    # concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))as YearMonth,
    # convert(sum(CAST(price AS SIGNED)),CHAR) AS 'revenue', null as
    # direct_cost, count(dateofsale) as number_sold,null number_donated,NOW()
    # as data_refresh_time
    # FROM autolog.autolog
    # group by  year (dateofsale),month (dateofsale) ,
    # concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))
    # union all
    # select 'SF' operation, year (date) Year,month (date) Month,
    # concat(year(date),'-',LPAD(month (date),2,'0'))as YearMonth, null AS
    # 'revenue', null as direct_cost, null number_sold, count(date) as
    # number_donated,NOW() as data_refresh_time
    # FROM autolog.autolog
    # group by year (date) ,month (date) , concat(year(date),'-',LPAD(month
    # (date),2,'0'))
    # union all
    # select 'LA' operation, year (dateofsale) Year,month (dateofsale) Month, 
    # concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))as YearMonth,
    # convert(sum(CAST(price AS SIGNED)),CHAR) AS 'revenue', null as
    # direct_cost, count(dateofsale) as number_sold,null number_donated,NOW()
    # as data_refresh_time
    # FROM autolog_la.autolog
    # group by  year (dateofsale),month (dateofsale) ,
    # concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))
    # union all
    # select 'LA' operation, year (date) Year,month (date) Month,
    # concat(year(date),'-',LPAD(month (date),2,'0'))as YearMonth, null AS
    # 'revenue', null as direct_cost, null number_sold, count(date) as
    # number_donated,NOW() as data_refresh_time
    # FROM autolog_la.autolog
    # group by year (date) ,month (date) , concat(year(date),'-',LPAD(month
    # (date),2,'0'))
    # union all
    # select 'ETC' operation, year (dateofsale) Sold_Year,month (dateofsale)
    # Sold_month,  concat(year(dateofsale),'-',LPAD(month
    # (dateofsale),2,'0'))as Sold_Yearmonth, convert(sum(CAST(price AS
    # SIGNED)),CHAR) AS 'revenue', null as direct_cost, count(dateofsale) as
    # number_sold,null number_donated,NOW() as data_refresh_time
    # FROM autolog_etc.autolog
    # group by  year (dateofsale),month (dateofsale) ,
    # concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))
    # union all
    # select 'ETC' operation, year (date) Sold_Year,month (date) Sold_month, 
    # concat(year(date),'-',LPAD(month (date),2,'0'))as Sold_Yearmonth, null
    # AS 'revenue', null as direct_cost, null number_sold, count(date) as
    # number_donated,NOW() as data_refresh_time
    # FROM autolog_etc.autolog
    # group by year (date) ,month (date) , concat(year(date),'-',LPAD(month
    # (date),2,'0'))
    # union all
    # select 'korea' operation, year (dateofsale) Sold_Year,month (dateofsale)
    # Sold_month,  concat(year(dateofsale),'-',LPAD(month
    # (dateofsale),2,'0'))as Sold_Yearmonth, convert(sum(CAST(price AS
    # SIGNED)),CHAR) AS 'revenue', null as direct_cost, count(dateofsale) as
    # number_sold,null number_donated,NOW() as data_refresh_time
    # FROM autolog_korea.autolog
    # group by  year (dateofsale),month (dateofsale) ,
    # concat(year(dateofsale),'-',LPAD(month (dateofsale),2,'0'))
    # union all
    # select 'korea' operation, year (date) Sold_Year,month (date)
    # Sold_month,  concat(year(date),'-',LPAD(month (date),2,'0'))as
    # Sold_Yearmonth, null AS 'revenue', null as direct_cost, null
    # number_sold, count(date) as number_donated,NOW() as data_refresh_time
    # FROM autolog_korea.autolog
    # group by year (date) ,month (date) , concat(year(date),'-',LPAD(month
    # (date),2,'0'))
    # union all
    # select operations, year (service_date) Year, month (service_date)
    # Month,  concat(year(service_date),'-',LPAD(month (service_date),2,'0'))
    # as YearMonth,
    # null AS 'revenue', sum(amounts) as direct_cost, count(service_date) as
    # number_sold, null number_donated, NOW() as data_refresh_time
    # FROM autolog.direct_cost
    # group by operations, year (service_date), month (service_date),
    # concat(year(service_date),'-',LPAD(month (service_date),2,'0'))
    # ) as CTE_ALL_DATA
    # group by
    # operation, Year, Month, YearMonth, data_refresh_time;"""

    # sql3="""select operations, Year, Month, YearMonth, data_refresh_time, amounts,cost_type as cost_type
    # from
    # (
    # select operations, year (service_date) Year, month (service_date)
    # Month,  concat(year(service_date),'-',LPAD(month (service_date),2,'0'))
    # as YearMonth, amounts as amounts, cost_type as cost_type, NOW() as data_refresh_time
    # FROM autolog.direct_cost
    # group by operations, year (service_date), month (service_date),
    # concat(year(service_date),'-',LPAD(month (service_date),2,'0'))
    # ) as CTE_ALL_DATA
    # group by
    # operations, Year, Month, YearMonth, data_refresh_time;"""

    mycursor.execute(sql2)
    myresult = mycursor.fetchall()
    df = pd.DataFrame(myresult)
    
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df = df.astype({col: 'string'})
    for col in df.columns:
        if df[col].dtype == 'object':
            df = df.astype({col: 'string'})

    for col in df.columns:
         print(df[col].dtype)
    export_pandas_df_to_sheets(spreadsheet_Id, df)


#defining the main function
def main():
    #step 4:  start the execution
    try:    
            
        #schedule.every().day.at("11:23").do(sqlQuering)
        ######schedule.every(0.1).minutes.do(sqlQuering)
        schedule.every().day.at("11:00").do(sqlQuering)
        while True:
        # Checks whether a scheduled task is pending to run or not
            schedule.run_pending()
            sleep(config.sleep_time)
    except Exception as e:
        print(e)
        # print("Connection failed!")


# Using the special variable
# __name__
if __name__=="__main__":
    #start defining variable
    load_dotenv()
    sheet_name="Direct_cost_withprofit"
    spreadsheet_Id="1IHVCpBFM_t4Bt8TC5-tlC3sGL0hKeioCy7PCZC-6PJ8"
    range_name = "Direct_cost_withprofit!A1"
    data_range = "Direct_cost_withprofit!A2"
    sheetId=1919431224
    
    main()