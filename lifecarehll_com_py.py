import os
import re
import sqlite3
import time
import warnings
import pyodbc
import requests
import logging
from zipfile import ZipFile
from datetime import datetime
from selenium import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

warnings.simplefilter("ignore")
driver = webdriver.Chrome(ChromeDriverManager().install())


def RemoveExtraSpace(Content):
    new = re.sub(' +', ' ', Content)
    return new


def generate_file_name(url):
    file_name = url.split('://')[1].split("/")[0].replace(".", "_")
    return file_name


def download_file_dir(url):
    # ----------------------------------------------pdf Down path --------------------------------------------#
    files_dir = os.path.expanduser('~') + "\\Documents\\" + "PythonFile\\" + url.split('://')[1].split("/")[0] + "_py\\" + "File"
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
    # ----------------------------------------------download pdf-------------------------------------------------#
    response = requests.get(links, verify=False)  # ------ verify-use openssl's error
    fullname = os.path.join(files_dir, datetime.now().strftime(f"%d%m%Y_%H%M%S%f") + "." + links.rsplit('.', 1)[-1])
    if response.status_code == 404 or response.status_code == 400:  # -- file downloaded bt file open and error 404 & 400
        fullname = None
        print('Error 404 and 400')
        logging.info('Error 404 and 400')

    else:
        pdf = open(fullname, 'wb')
        pdf.write(response.content)
        pdf.close()
        logging.info("File Downloaded")
    return fullname


def sqlite_and_sql_server_db(Page_Data, create_db_and_table_name):
    try:
        # ----------------------------------------------insert data in sqlite table ----------------------------------#
        que = "INSERT INTO " + create_db_and_table_name + " (Tender_Notice_No, Tender_Summery, Tender_Details, Bid_deadline_2, Documents_2, Competition, Purchaser_Name, Pur_Add, Pur_URL, OpeningDate, Flag) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)"
        conn.executemany(que, Page_Data)
        conn.commit()
        logging.info('SQLite Data Inserted')
#
        # ----------------------------------------------select sqlite data for insert in sql server ------------------#
        sqlite_rows = conn.execute("SELECT Tender_Notice_No, Tender_Summery, Tender_Details, Bid_deadline_2, Documents_2, Competition, Purchaser_Name, Pur_Add, Pur_URL, OpeningDate FROM " + create_db_and_table_name + " WHERE Flag = 1").fetchall()
        if len(sqlite_rows) > 0:
            sql_conn = pyodbc.connect('Driver={SQL Server};'
                                      'Server=192.168.100.153;'
                                      'Database=CrawlingDB;'
                                      'UID=anjali;'
                                      'PWD=anjali@123;')
            sql_cursor = sql_conn.cursor()

            # ----------------------------------------------table create in sql server -------------------
            sql_cursor.execute("IF NOT EXISTS (SELECT name FROM sysobjects WHERE name= '" + create_db_and_table_name + "' AND xtype='U') CREATE TABLE " + create_db_and_table_name + " (id int IDENTITY(1,1) PRIMARY KEY, Tender_Notice_No TEXT, Tender_Summery TEXT, Tender_Details TEXT, Bid_deadline_2 TEXT, Documents_2 TEXT, TenderListing_key TEXT, Notice_Type TEXT, Competition TEXT, Purchaser_Name TEXT, Pur_Add TEXT, Pur_State TEXT, Pur_City TEXT, Pur_Country TEXT, Pur_Email TEXT, Pur_URL TEXT, Bid_Deadline_1 TEXT, Financier_Name TEXT, CPV TEXT, scannedImage TEXT, Documents_1 TEXT, Documents_3 TEXT, Documents_4 TEXT, Documents_5 TEXT,currency TEXT, actualvalue TEXT, TenderFor TEXT, TenderType TEXT, SiteName TEXT, createdOn TEXT, updateOn TEXT, Content TEXT, Content1 TEXT, Content2 TEXT, Content3 TEXT, DocFees TEXT, EMD TEXT, OpeningDate TEXT, Tender_No TEXT)")

            # ----------------------------------------------insert data in sql server db -----------------------------#
            que1 = "INSERT INTO " + create_db_and_table_name + " (Tender_Notice_No, Tender_Summery, Tender_Details, Bid_deadline_2, Documents_2, Competition, Purchaser_Name, Pur_Add, Pur_URL, OpeningDate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            sql_cursor.executemany(que1, sqlite_rows)
            sql_conn.commit()
            logging.info('SQL Server Data Inserted')
            sql_conn.close()

            # ----------------------------------------------update sqlite db for flag 1-------------------------------#
            conn.execute("UPDATE " + create_db_and_table_name + " SET Flag = 0 WHERE Flag = 1")
            conn.commit()

    except Exception as e:
        print(e)
        logging.error('Error occurred ' + str(e))


try:
    url = 'http://www.lifecarehll.com/tender'
    print(url)

    create_db_and_table_name = url.split('://')[1].split("/")[0].replace(".", "_") + '_py'

    file_name = generate_file_name(url)
    conn = create_db(file_name, create_db_and_table_name)
    log_file_path = log_file_dir()
    files_dir = download_file_dir(url)

    logging.basicConfig(filename=log_file_path + file_name + ".log", filemode='a', level=logging.INFO,
                        format='%(asctime)s %(message)s')
    logging.info('Started Web Scraping')
    logging.info('Program Start')

    Competition = "NCB"
    Purchaser_Name = "Indian Railways"
    Pur_Add = "Indian Railways"
    Pur_URL = url

    Page_Data = []
    while True:
        driver.get(url)
        tr_data = driver.find_elements(By.XPATH, '//*[@class="table_general"]//tbody//tr')

        del tr_data[0]
        for tr_d in tr_data:

            t_num = tr_d.find_element(By.XPATH, ".//td[3]").text.strip()
            t_sum = tr_d.find_element(By.XPATH, ".//td[2]").text.strip()

            row_exists = "SELECT * FROM " + create_db_and_table_name + " WHERE Tender_Notice_No='" + t_num + "'"
            if not conn.execute(row_exists).fetchone():
                logging.info("Fresh")

                all_links = driver.find_elements(By.XPATH, '//*[@class="table_general"]//tbody/tr//td[2]/a')
                for i in all_links:
                    i.send_keys(Keys.CONTROL + Keys.RETURN)  # open new tab and return old tab
                    driver.switch_to.window(driver.window_handles[1])
                    driver.implicitly_wait(0.5)

                    Tender_Notice_No  = driver.find_element(By.XPATH, '//*[@id="search_content"]/div[2]/table/tbody/tr[3]/td[2]').text.strip()
                    print(Tender_Notice_No )

                    Tender_Summery = driver.find_element(By.XPATH, '//*[@id="search_content"]/div[2]/table/tbody/tr[4]/td[2]').text.strip()
                    print(Tender_Summery)

                    Tender_Details = driver.find_element(By.XPATH, '//*[@id="search_content"]/div[4]/table/tbody/tr[2]/td[2]').text.strip()
                    print(Tender_Details)

                    OpeningDate = driver.find_element(By.XPATH, '//*[@id="search_content"]/div[3]/table/tbody/tr[5]/td[2]').text.strip()
                    print(OpeningDate)

                    Bid_deadline_2 = driver.find_element(By.XPATH, '//*[@id="search_content"]/div[3]/table/tbody/tr[4]/td[2]').text.strip()
                    print(Bid_deadline_2)

                    links = []
                    links_data = driver.find_elements(By.XPATH, '//*[@id="search_content"]/div[5]/table/tbody/tr/td/a')
                    for lnk in links_data:
                        lnk = lnk.get_attribute('href')
                        links.append(lnk)
                    links = ', '.join(links)
                    print(links)

                    Documents_2 = download_pdf(links)
                    Row_data = [Tender_Notice_No, Tender_Summery, Tender_Details, Bid_deadline_2, Documents_2, Competition, Purchaser_Name, Pur_Add, Pur_URL, OpeningDate]
                    Page_Data.append(Row_data)

            else:
                print("This Tender_Notice_No is already exist.")
                logging.info("Duplicated")

        url = driver.find_element(By.XPATH, value='//*[contains(text(), "Next")]').get_attribute('href')
        print(url)

        if url == None:
            break

    sqlite_and_sql_server_db(Page_Data, create_db_and_table_name)

    conn.close()
    driver.close()
    logging.info('Proceed Successfully Done')
    print("Completed")

except Exception as e:
    print(e)
