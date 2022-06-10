import os
import sqlite3
import warnings
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import logging
import pyodbc
import re
from webdriver_manager.chrome import ChromeDriverManager

warnings.simplefilter("ignore")
driver = webdriver.Chrome(ChromeDriverManager().install())


def RemoveExtraSpace(Content):
    new = re.sub(' +', ' ', Content)
    return new


def generate_file_name(url):
    file_name = url.split('http://')[1].split("/")[0].replace(".", "_")
    return file_name


def download_file_dir(url):
    # ----------------------------------------------pdf Down path --------------------------------------------#
    files_dir = os.path.expanduser('~') + "\\Documents\\" + "PythonFile\\" + url.split('http://')[1].split("/")[0] + "py\\" + "File"
    if os.path.exists(files_dir):
        pass
    else:
        os.makedirs(files_dir)

    return files_dir


def log_file_dir():
    log_file_path = os.path.expanduser('~') + "\\Documents\\" + "PythonLog\\"
    if os.path.exists(log_file_path):
        pass
    else:
        os.makedirs(log_file_path)
    return log_file_path


def create_db(file_name, create_db_and_table_name):
    # ----------------------------------------------database created using sqlite ---------------------------------#
    conn = sqlite3.connect(file_name + '.db')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS " + create_db_and_table_name + "(Id INTEGER PRIMARY KEY, Tender_Notice_No TEXT, Tender_Summery TEXT, Tender_Details TEXT, Bid_deadline_2 TEXT, Documents_2 TEXT, TenderListing_key TEXT, Notice_Type TEXT, Competition TEXT, Purchaser_Name TEXT, Pur_Add TEXT, Pur_State TEXT, Pur_City TEXT, Pur_Country TEXT, Pur_Email TEXT, Pur_URL TEXT, Bid_Deadline_1 TEXT, Financier_Name TEXT, CPV TEXT, scannedImage TEXT, Documents_1 TEXT, Documents_3 TEXT, Documents_4 TEXT, Documents_5 TEXT, currency TEXT, actualvalue TEXT, TenderFor TEXT, TenderType TEXT, SiteName TEXT, createdOn TEXT, updateOn TEXT, Content TEXT, Content1 TEXT, Content2 TEXT, Content3 TEXT, DocFees TEXT, EMD TEXT, OpeningDate TEXT, Tender_No TEXT, Flag TEXT)")
    return conn


def download_pdf(links):
    try:
        response = requests.get(links)
        fullname = os.path.join(files_dir, datetime.now().strftime(f"%d%m%Y_%H%M%S%f") + "." + links.rsplit('.', 1)[-1])

        pdf = open(fullname, 'wb')
        pdf.write(response.content)
        pdf.close()
        logging.info("File Downloaded")

        return fullname

    except Exception as e:
        print(e)
        logging.error('Error occurred ' + str(e))


def sqlite_to_sql_server(Page_Data, create_db_and_table_name):
    try:
        que = "INSERT INTO " + create_db_and_table_name + " (Tender_Notice_No, Tender_Summery, Bid_deadline_2, Documents_2, Competition, Purchaser_Name, Pur_Add, Pur_URL, Content, Flag) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)"
        conn.executemany(que, Page_Data)
        conn.commit()
        logging.info('SQLite Data Inserted')

        sqlite_rows = conn.execute("SELECT Tender_Notice_No, Tender_Summery, Bid_deadline_2, Documents_2, Competition, Purchaser_Name, Pur_Add, Pur_URL, Content FROM " + create_db_and_table_name + " WHERE Flag = 1").fetchall()
        if len(sqlite_rows) > 0:
            sql_conn = pyodbc.connect('Driver={SQL Server};'
                                      'Server=192.168.100.153;'
                                      'Database=CrawlingDB;'
                                      'UID=anjali;'
                                      'PWD=anjali@123;')
            sql_cursor = sql_conn.cursor()
            sql_cursor.execute("IF NOT EXISTS (SELECT name FROM sysobjects WHERE name= '" + create_db_and_table_name + "' AND xtype='U') CREATE TABLE " + create_db_and_table_name + " (id int IDENTITY(1,1) PRIMARY KEY, Tender_Notice_No TEXT, Tender_Summery TEXT, Tender_Details TEXT, Bid_deadline_2 TEXT, Documents_2 TEXT, TenderListing_key TEXT, Notice_Type TEXT, Competition TEXT, Purchaser_Name TEXT, Pur_Add TEXT, Pur_State TEXT, Pur_City TEXT, Pur_Country TEXT, Pur_Email TEXT, Pur_URL TEXT, Bid_Deadline_1 TEXT, Financier_Name TEXT, CPV TEXT, scannedImage TEXT, Documents_1 TEXT, Documents_3 TEXT, Documents_4 TEXT, Documents_5 TEXT,currency TEXT, actualvalue TEXT, TenderFor TEXT, TenderType TEXT, SiteName TEXT, createdOn TEXT, updateOn TEXT, Content TEXT, Content1 TEXT, Content2 TEXT, Content3 TEXT, DocFees TEXT, EMD TEXT, OpeningDate TEXT, Tender_No TEXT)")

            que1 = "INSERT INTO " + create_db_and_table_name + " (Tender_Notice_No, Tender_Summery, Bid_deadline_2, Documents_2, Competition, Purchaser_Name, Pur_Add, Pur_URL, Content) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            sql_cursor.executemany(que1, sqlite_rows)
            sql_conn.commit()
            logging.info('SQL Server Data Inserted')
            sql_conn.close()

            conn.execute("UPDATE " + create_db_and_table_name + " SET Flag = 0 WHERE Flag = 1")
            conn.commit()
    except Exception as e:
        print(e)
        logging.error('Error occurred ' + str(e))


try:

    url = 'http://bstdc.bihar.gov.in/tenders.htm'
    driver.get(url)

    create_db_and_table_name = url.split('http://')[1].split("/")[0].replace(".", "_") + '_py'

    file_name = generate_file_name(url)
    conn = create_db(file_name, create_db_and_table_name)
    log_file_path = log_file_dir()
    files_dir = download_file_dir(url)

    logging.basicConfig(filename=log_file_path + file_name + ".log", filemode='a', level=logging.DEBUG, format='%(asctime)s %(message)s')
    logging.info('Started Web Scraping')
    logging.info('Program Start')

    tr_all_data = driver.find_elements(By.XPATH, value='//tbody//tr')

    Competition = "NCB"
    Purchaser_Name = "Bihar State Tourism Development Corporation"
    Pur_Add = "Bihar State Tourism Development Corporation"
    Pur_URL = url

    Page_Data = []
    for tr_data in tr_all_data:

        if not 'Corrigendum' in tr_data.text:
            Tender_Notice_No = tr_data.find_element(By.XPATH, value='./td[1]/span/a').text
            logging.info(f'{Tender_Notice_No}')
            links = tr_data.find_element(By.XPATH, value='./td[1]/span/a').get_attribute('href')
            Bid_deadline_2 = tr_data.find_element(By.XPATH, value='./td[3]').text
            Tender_Summery = tr_data.find_element(By.XPATH, value='./td[2]').text
            Content = tr_data.get_attribute('innerHTML')
            Content = RemoveExtraSpace(Content)

            row_exists = "SELECT * FROM " + create_db_and_table_name + " WHERE Tender_Notice_No='" + Tender_Notice_No + "' AND Tender_Summery='" + Tender_Summery + "' AND Bid_deadline_2='" + Bid_deadline_2 + "'"
            if not conn.execute(row_exists).fetchone():
                logging.info("Fresh")
                Documents_2 = download_pdf(links)

                Row_data = [Tender_Notice_No, Tender_Summery, Bid_deadline_2, Documents_2, Competition, Purchaser_Name, Pur_Add, Pur_URL, Content]
                Page_Data.append(Row_data)

            else:
                print("This Tender_Notice_No, Tender_Summery, Bid_deadline_2 is already exist.")
                logging.info("Duplicated")

    sqlite_to_sql_server(Page_Data, create_db_and_table_name)

    conn.close()
    driver.close()
    logging.info('Proceed Successfully Done')
    print("completed")

except Exception as e:
    print(e)
    logging.error('Error occurred ' + str(e))
