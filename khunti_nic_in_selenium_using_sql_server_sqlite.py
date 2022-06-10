import os
import sqlite3
import warnings
from colorama import Cursor
from numpy import row_stack
import requests
from zipfile import ZipFile
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import logging
import pyodbc
from webdriver_manager.chrome import ChromeDriverManager

warnings.simplefilter("ignore")
# driver = webdriver.Chrome(ChromeDriverManager().install())
driver = webdriver.Chrome(r'C:\Users\kansara.anjali\Desktop\chromedriver_win32\chromedriver.exe')


def generate_file_name(khunti_url):
    file_name = khunti_url.split('https://')[1].split("/")[0].replace(".", "_")
    return file_name

def create_csv(file_name):
    # create csv file only with headers
    data = {'Title': [], 'Description': [], 'StartDate': [], 'EndDate': [], 'File': [], 'DownloadFilePath': [], 'SourceUrl': []}
    pd.DataFrame(data).to_csv(file_name + ".csv", index=False)

def create_db(file_name):
    conn = sqlite3.connect(file_name + '.db')
    Cursor = conn.cursor()

    create_db_query = """CREATE TABLE khunti_nic_in(Id INTEGER PRIMARY KEY,
                                                        Tender_Notice_No TEXT,
                                                        Tender_Summery TEXT,
                                                        Tender_Details TEXT,
                                                        Bid_deadline_2 TEXT,
                                                        Documents_2 TEXT,
                                                        TenderListing_key TEXT,
                                                        Notice_Type TEXT,
                                                        Competition TEXT,
                                                        Purchaser_Name TEXT,
                                                        Pur_Add TEXT,
                                                        Pur_State TEXT,
                                                        Pur_City TEXT,
                                                        Pur_Country TEXT,
                                                        Pur_Email TEXT,
                                                        Pur_URL TEXT,
                                                        Bid_Deadline_1 TEXT,
                                                        Financier_Name TEXT,
                                                        CPV TEXT,
                                                        scannedImage TEXT,
                                                        Documents_1 TEXT,
                                                        Documents_3 TEXT,
                                                        Documents_4 TEXT,
                                                        Documents_5 TEXT,
                                                        currency TEXT,
                                                        actualvalue TEXT,
                                                        TenderFor TEXT,
                                                        TenderType TEXT,
                                                        SiteName TEXT,
                                                        createdOn TEXT,
                                                        updateOn TEXT,
                                                        Content TEXT,
                                                        Content1 TEXT,
                                                        Content2 TEXT,
                                                        Content3 TEXT,
                                                        DocFees TEXT,
                                                        EMD TEXT,
                                                        OpeningDate TEXT,
                                                        Tender_No TEXT,
                                                        Flag TEXT)"""
                                                        

    tb_exists = "SELECT name FROM sqlite_master WHERE type='table' AND name='khunti_nic_in'"

    if not Cursor.execute(tb_exists).fetchone():
        Cursor.execute(create_db_query)
    # else:
    #     print('Table Already Exists! Now, You Can Insert Date.')
    return conn

def download_pdf(links):
    # ***************** one by one File Download of any extension *****************
    all_links = links.split(',')
    if len(all_links) > 1:
        fullname = os.path.join(files_dir, datetime.now().strftime(f"%d%m%Y_%H%M%S%f") + '.zip')
        # print(fullname + ">>>" + links)
        zip_obj = ZipFile(fullname, 'w')

        for pdfLink in all_links:
            filename = datetime.now().strftime(f"%d%m%Y_%H%M%S%f") + "." + pdfLink.rsplit('.', 1)[-1]
            response = requests.get(pdfLink)

            pdf = open(filename, 'wb')
            pdf.write(response.content)
            pdf.close()

            zip_obj.write(filename)
            os.remove(filename)
            logging.info("Zip File Downloaded")

    else:
        response = requests.get(links)
        fullname = os.path.join(files_dir, datetime.now().strftime(f"%d%m%Y_%H%M%S%f") + "." + links.rsplit('.', 1)[-1])

        pdf = open(fullname, 'wb')
        pdf.write(response.content)
        pdf.close()
        logging.info("File Downloaded")

    return fullname

def insert_not_exist_data(file_name):
    # ----------------------------------------  check duplicate using db and csv ------------------------------------------------
    row_exists = "SELECT * FROM khunti_nic_in WHERE Tender_Summery='" + Tender_Summery + "' AND OpeningDate='" + OpeningDate + "' AND Bid_deadline_2='" + Bid_deadline_2 + "'"

    if not conn.execute(row_exists).fetchone():

        #download pdf after checking
        fullname = download_pdf(links)
        Documents_2 = fullname
        Pur_URL = khunti_url

        conn.execute("INSERT INTO khunti_nic_in(Tender_Summery, Tender_Details, OpeningDate, Bid_deadline_2, Documents_2, Pur_URL, Flag) VALUES (?, ?, ?, ?, ?, ?, ?)", (Tender_Summery, Tender_Details, OpeningDate, Bid_deadline_2, Documents_2, Pur_URL, '1',))
        conn.commit()       

        logging.info("Fresh")
        logging.info("Database Data Inserted")

        # append dataframe to  csv
        data = {'Tender_Summery': [str(Tender_Summery)], 'Tender_Details': [str(Tender_Details)], 'OpeningDate': [str(OpeningDate)], 'Bid_deadline_2': [str(Bid_deadline_2)], 'Documents_2': [str(Documents_2)], 'Pur_URL': [str(Pur_URL)]}        
        pd.DataFrame(data).to_csv(file_name + ".csv", mode='a', index=False, header=False)
        logging.info("Excel Data Inserted")

    else:
        print("This Title, StartDate, EndDate is already exist.")
        logging.info("Duplicated")
    
def sqlite_to_sql_server():

    sqlite_rows = conn.execute("SELECT Tender_Summery, Tender_Details, OpeningDate, Bid_deadline_2, Documents_2, Pur_URL FROM khunti_nic_in WHERE Flag = 1").fetchall()
    if len(sqlite_rows) > 0:
            
        sql_conn = pyodbc.connect('Driver={SQL Server};'
                                'Server=192.168.100.153;'
                                'Database=CrawlingDB;'
                                'UID=anjali;'
                                'PWD=anjali@123;')
        sql_cursor = sql_conn.cursor()
        sql_cursor.execute("""IF NOT EXISTS (SELECT name FROM sysobjects WHERE name='khunti_nic_in' AND xtype='U') CREATE TABLE khunti_nic_in (id int IDENTITY(1,1) PRIMARY KEY, 
                                                                                                                                                Tender_Notice_No TEXT,                                                                                                                                
                                                                                                                                                Tender_Summery TEXT,
                                                                                                                                                Tender_Details TEXT,
                                                                                                                                                Bid_deadline_2 TEXT,
                                                                                                                                                Documents_2 TEXT,
                                                                                                                                                TenderListing_key TEXT,
                                                                                                                                                Notice_Type TEXT,
                                                                                                                                                Competition TEXT,
                                                                                                                                                Purchaser_Name TEXT,
                                                                                                                                                Pur_Add TEXT,
                                                                                                                                                Pur_State TEXT,
                                                                                                                                                Pur_City TEXT,
                                                                                                                                                Pur_Country TEXT,
                                                                                                                                                Pur_Email TEXT,
                                                                                                                                                Pur_URL TEXT,
                                                                                                                                                Bid_Deadline_1 TEXT,
                                                                                                                                                Financier_Name TEXT,
                                                                                                                                                CPV TEXT,
                                                                                                                                                scannedImage TEXT,
                                                                                                                                                Documents_1 TEXT,
                                                                                                                                                Documents_3 TEXT,
                                                                                                                                                Documents_4 TEXT,
                                                                                                                                                Documents_5 TEXT,
                                                                                                                                                currency TEXT,
                                                                                                                                                actualvalue TEXT,
                                                                                                                                                TenderFor TEXT,
                                                                                                                                                TenderType TEXT,
                                                                                                                                                SiteName TEXT,
                                                                                                                                                createdOn TEXT,
                                                                                                                                                updateOn TEXT,
                                                                                                                                                Content TEXT,
                                                                                                                                                Content1 TEXT,
                                                                                                                                                Content2 TEXT,
                                                                                                                                                Content3 TEXT,
                                                                                                                                                DocFees TEXT,
                                                                                                                                                EMD TEXT,
                                                                                                                                                OpeningDate TEXT,
                                                                                                                                                Tender_No TEXT)""")
                                                                                                                                          
        que1 = "INSERT INTO khunti_nic_in (Tender_Summery, Tender_Details, OpeningDate, Bid_deadline_2, Documents_2, Pur_URL) VALUES (?, ?, ?, ?, ?, ?)"  
        sql_cursor.executemany(que1, sqlite_rows)
        sql_conn.commit()
        logging.info('SQL Server Data Inserted')
        sql_conn.close()    

        conn.execute("UPDATE khunti_nic_in SET Flag = 0 WHERE Flag = 1")
        conn.commit()
                                                                                                                                                        
try:

    files_dir = os.path.expanduser('~') + "\\Documents\\" + "PythonFile\\" + "File"
    if os.path.exists(files_dir):
        pass
    else:
        os.makedirs(files_dir)

    khunti_url = 'https://khunti.nic.in/past-notices/tenders/'

    file_name = generate_file_name(khunti_url)
    create_csv(file_name)
    conn = create_db(file_name)    

    logging.basicConfig(filename=file_name + '.log', filemode='a', level=logging.DEBUG, format='%(asctime)s %(message)s')
    logging.info('Started Web Scraping')
    logging.info('Program Start')

    while True:
        driver.get(khunti_url)
        print(khunti_url)

        tr_all_data = driver.find_elements(By.XPATH, value='//tbody//tr')
        for tr_data in tr_all_data:
            Tender_Summery = tr_data.find_element(By.XPATH, value='./td[1]').text
            Tender_Details = tr_data.find_element(By.XPATH, value='./td[2]').text
            OpeningDate = tr_data.find_element(By.XPATH, value='./td[3]').text
            Bid_deadline_2 = tr_data.find_element(By.XPATH, value='./td[4]').text
            td5 = tr_data.find_elements(By.XPATH, value='./td[5]/span/span//*[@title="Click here to View"]')
            links = []
            for td_five_data in td5:
                td_five_data = td_five_data.get_attribute('href')
                links.append(td_five_data)
            links = ', '.join(links)
            # col_file = str(links)              #### col not use in sql server so hide this column col_file_pdf_url
            # print(col_file)

            insert_not_exist_data(file_name)            

        khunti_url = driver.find_element(By.XPATH, value='//*[contains(text(), "Next")]').get_attribute('href')
        if khunti_url == None:
            break

    sqlite_to_sql_server()
    # conn.execute("UPDATE khunti_nic_in SET Flag = 0 WHERE Flag = 1")
    # conn.commit()
    conn.close()
    driver.close()
    logging.info('Proceed Successfully Done')

except Exception as e:
    print(e)
    logging.error('Error occurred ' + str(e))






