from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
import time
import pandas as pd
from  datetime import date
import mysql_connection
import logs.logger_config

import logging
logger = logging.getLogger(__name__)

def extract_bulk_deals_information():
    try:
        ### open the browser 
        browser = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
        browser.maximize_window() 

        ##open the website using the  browser
        url = 'https://www.bseindia.com/markets/equity/EQReports/bulk_deals.aspx'
        browser.get(url)

        time.sleep(5) ## waiting for the page to load

        logger.info(f"Browser opened for the website:- {url}")

        deal_date_list = browser.find_elements(by='xpath' , value='//*[@id="ContentPlaceHolder1_gvbulk_deals"]/tbody/tr/td[1]')
        deal_date = [deal_date.text  if deal_date.text != '' else '-' for deal_date in deal_date_list]

        security_code_list  = browser.find_elements(by='xpath' , value='//*[@id="ContentPlaceHolder1_gvbulk_deals"]/tbody/tr/td[2]')
        security_code = [security_code.text  if security_code.text != '' else '-' for security_code in security_code_list]

        security_name_list = browser.find_elements(by='xpath' , value='//*[@id="ContentPlaceHolder1_gvbulk_deals"]/tbody/tr/td[3]')
        security_name = [security_name.text  if security_name.text != '' else '-' for security_name in security_name_list]

        client_name_list = browser.find_elements(by='xpath' , value='//*[@id="ContentPlaceHolder1_gvbulk_deals"]/tbody/tr/td[4]')
        client_name = [client_name.text  if client_name.text != '' else '-' for client_name in client_name_list]


        deal_type_list = browser.find_elements(by='xpath' , value='//*[@id="ContentPlaceHolder1_gvbulk_deals"]/tbody/tr/td[5]')
        deal_type = [deal_type.text  if deal_type.text != '' else '-' for deal_type in deal_type_list]

        quantity_list = browser.find_elements(by='xpath' , value='//*[@id="ContentPlaceHolder1_gvbulk_deals"]/tbody/tr/td[6]')
        quantity = [quantity.text  if quantity.text != '' else '-' for quantity in quantity_list]

        price_list = browser.find_elements(by='xpath' , value='//*[@id="ContentPlaceHolder1_gvbulk_deals"]/tbody/tr/td[7]')
        price = [price.text  if price.text != '' else '-' for price in price_list]
        
        logger.info("Fetched the required information from the browser")
        #### creating the dataframe 
        df = pd.DataFrame(zip(deal_date , security_code ,security_name ,client_name ,deal_type , quantity , price) ,
                columns=['deal_date' , 'security_code' ,'security_name' ,'client_name' ,'deal_type' , 'quantity' , 'price'])
        df['created_date'] = date.today()

        logger.info("DataFrame is Created for the extracted data")

        return {
            "Status" : "Success" ,
            "Result" : df ,
            "Message" : "Extracted the data successfully"
        }


    except Exception as e:
        logger.error(f"Exception while extracting the information : {e}")
        context = {
            "Status" : "Failure" ,
            "Messsage" :f"Exception while extracting the information : {e}"
        }
        return context


""" Inserting the data (pandas dataframe ) into the mysql database"""
def insert_data_into_db(df):
    try:
        engine = mysql_connection.engine
        res = df.to_sql('bulk_deal' , con = engine , if_exists='append' , index=False)
        
        return {
            "Status" :"Success" ,
            "Message" :"Data inserted sucessfully into database" ,
            "Result" : res
        } 

    except Exception as e:
        return {
            "Status" :"Failure" ,
            "Message" : f"Exception while inserting datainto the database{e}",
            "Result" : None
        }

