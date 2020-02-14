import os
import pandas as pd
import glob
import sys
import threading
import logging
from io import BytesIO

# To parse the HTML page, BeautifulSoup library is used

from bs4 import BeautifulSoup

# To read and open zipfile, zipfile module is used

from urllib.request import urlopen
from zipfile import ZipFile

# Amazon S3

import boto.s3
import boto3

import datetime
import time

# Step 9: This function calculates the summary statistics of the columns in the dataframe

def summary(all_data):
    data = pd.DataFrame()
    data = all_data
    
    
    # summary = pd.DataFrame()
    logging.debug('In the function : summary')
    csvpath=str(os.getcwd())
    
    # add Timestamp for the analysis purpose
    data['Timestamp'] = data[['date', 'time']].astype(str).sum(axis=1)
    # Create a summary that groups ip by date
    
    summary1=data['ip'].groupby(data['date']).describe()
    summaryipdescribe = pd.DataFrame(summary1)
    s=summaryipdescribe.transpose()
    s.to_csv(csvpath+"/summaryipbydatedescribe.csv")
    
    # Create a summary that groups cik by accession number
    summary2 = data['extention'].groupby(data['cik']).describe()
    summarycikdescribe = pd.DataFrame(summary2)
    summarycikdescribe.to_csv(csvpath+"/summarycikbyextentiondescribe.csv")
    
    # get Top 10 count of all cik with their accession number
    data['COUNT'] = 1  # initially, set that counter to 1.
    group_data = data.groupby(['date', 'cik', 'accession'])['COUNT'].count()  # sum function
    rankedData=group_data.rank()
    summarygroup=pd.DataFrame(rankedData)
    summarygroup.to_csv(csvpath+"/Top10cik.csv")
    
    
    # For anomaly detection -check the length of cik
    data['cik'] = data['cik'].astype('str')
    data['cik_length'] = data['cik'].str.len()
    data[(data['cik_length'] > 10)]
    data['COUNT'] = 1
    datagroup=pd.DataFrame(data)
    datagroup.to_csv(csvpath+"/LengthOfCikForAnomalyDetection.csv")
    
    
    # Per code count
    status = data.groupby(['code']).count()  # sum function
    status['COUNT']
    summary=pd.DataFrame(status)
    summary.to_csv(csvpath+"/PercodeCount.csv")
	
# Step 8: This function is to find out the missing values in the dataset and replace the missing values

def replace_missingValues(all_data):
    data = pd.DataFrame()
    logging.debug('In the function : replace_missingValues')
    all_data.loc[all_data['extention'] == '.txt', 'extention'] = all_data["accession"].map(str) + all_data["extention"]
    all_data['browser'] = all_data['browser'].fillna('win')
    all_data['size'] = all_data['size'].fillna(0)
    all_data['size'] = all_data['size'].astype('int64')
    all_data = pd.DataFrame(all_data.join(all_data.groupby('cik')['size'].mean(), on='cik', rsuffix='_newsize'))
    all_data['size_newsize'] = all_data['size_newsize'].fillna(0)
    all_data['size_newsize'] = all_data['size_newsize'].astype('int64')
    all_data.loc[all_data['size'] == 0, 'size'] = all_data.size_newsize
    del all_data['size_newsize']
    data = all_data
    return data
	
# Step 7: The datatype some columns in the dataframe is converted from floar to int for summary calculation 

def change_dataTypes(all_data):
    logging.debug('In the function : change_dataTypes')
    all_data['zone'] = all_data['zone'].astype('int64')
    all_data['cik'] = all_data['cik'].astype('int64')
    all_data['code'] = all_data['code'].astype('int64')
    all_data['idx'] = all_data['idx'].astype('int64')
    all_data['noagent'] = all_data['noagent'].astype('int64')
    all_data['norefer'] = all_data['norefer'].astype('int64')
    all_data['crawler'] = all_data['crawler'].astype('int64')
    all_data['find'] = all_data['find'].astype('int64')
    newdata = replace_missingValues(all_data)
    newdata.to_csv("merged.csv",encoding='utf-8')
    summary(newdata)
    return 0
	
# Step 6: The below function will create a dataframe of the extracted data from the zip file

def create_dataframe(path):
    logging.debug('In the function : create_dataframe')
    all_data = pd.DataFrame()
    # The below for loop will go through every file in the folder that  has been passed as input
    # And whose extention is .csv to create dataframes out of it
    for f in glob.glob(path + '/log*.csv'):
        df = pd.read_csv(f, parse_dates=[1])
        all_data = all_data.append(df, ignore_index=True)
    return all_data
	
# Step 5: The below function just makes sure that the path provided as the input exists

def assure_path_exists(path):
    logging.debug('In a function : assure_path_exists')
    if not os.path.exists(path):
        os.makedirs(path)
    return 0
	

# Step 4: The below function will get the download the zip files links from step 3 and store the data on the local
# This function will create a folder with the same name as the year entered by the user
# It will then iterate over every element in the list which stored the zip file links of the first day of every month
# It will then extract the zip file data


def get_dataOnLocal(monthlistdata, year):
    logging.debug('In the function : get_dataOnLocal')
    df = pd.DataFrame()
    # The below code will basically a create a folder of the same name as the year entered by the user
    foldername = str(year)
    path = str(os.getcwd()) + "/" + foldername
    # This is step 5 which will check whether this path is valid or not
    assure_path_exists(path)
    for month in monthlistdata:
        # The below code will extract the data from the zip file
        with urlopen(month) as zipresp:
            with ZipFile(BytesIO(zipresp.read())) as zfile:
                zfile.extractall(path)
    df = create_dataframe(path)
    # The below function is step 7 which will change the datatypes of the columns in the dataframe
    change_dataTypes(df)
    return 0
	
# Step 3: The below function is baiclaly passing the link generated in Step 2 which is the link of a particular year
# (cotd) entered by the user. Now this link has all the zip file for all the days in that year
# (cotd) But we are interested in only the first day of every month of that particular year
#(cotd) So this function will get us a list of the zip file links for the first day of every month for the year


def get_allmonth_data(linkhtml, year):
    logging.debug('In the function : get_allmonth_data')
    allzipfiles = BeautifulSoup(linkhtml, "html.parser")
    ziplist = allzipfiles.find_all('li')
    monthlistdata = []
    count = 0
    for li in ziplist:
        zipatags = li.findAll('a')
        for zipa in zipatags:
            if "01.zip" in zipa.text:
                monthlistdata.append(zipa.get('href'))
    get_dataOnLocal(monthlistdata, year)
    return 0
	
# Step 2 : Once you get the year number, you need to go to the main URL and get the url of that particular year
# Eg: if the year entered is 2006, the URL generated by this function is "https://www.sec.gov/files/edgar2006_1.html"

def get_url(year):
    logging.debug('In the function : get_url')
    url = 'https://www.sec.gov/data/edgar-log-file-data-set.html'
    html = urlopen(url)
    soup = BeautifulSoup(html, "html.parser")
    all_div = soup.findAll("div", attrs={'id': 'asyncAccordion'})
    for div in all_div:
        h2tag = div.findAll("a")
        for a in h2tag:
            if str(year) in a.get('href'):
                global ahref
                ahref = a.get('href')
    linkurl = 'https://www.sec.gov' + ahref
    logging.debug('Calling the initial URL')
    linkhtml = urlopen(linkurl)
    print(linkhtml)
    get_allmonth_data(linkhtml, year)
    return 0
	
# Step 1: The below function will check if the year entered by the user is a valid year or not

def valid_year(year):
    logging.debug('In the function : valid_year')
    logYear = ['2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015',
               '2016']
    for log in logYear:
        try:
            if year in log:
                get_url(year)
        except:
            print("Data for" + year + "does not exist")
            "Data for" + year + "does not exist"
    return 0
	
# The below code is basically uploading the log file and the zip file to Amazon S3

def upload_to_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,inputLocation,filepaths):
  
    try:
        conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        print("Connected to S3")
    except:
        logging.info("Amazon keys are invalid!!")
        print("Amazon keys are invalid!!")
        exit()
        
    loc=''

    if inputLocation == 'APNortheast':
        loc=boto.s3.connection.Location.APNortheast
    elif inputLocation == 'APSoutheast':
        loc=boto.s3.connection.Location.APSoutheast
    elif inputLocation == 'APSoutheast2':
        loc=boto.s3.connection.Location.APSoutheast2
    elif inputLocation == 'CNNorth1':
        loc=boto.s3.connection.Location.CNNorth1
    elif inputLocation == 'EUCentral1':
        loc=boto.s3.connection.Location.EUCentral1
    elif inputLocation == 'EU':
        loc=boto.s3.connection.Location.EU
    elif inputLocation == 'SAEast':
        loc=boto.s3.connection.Location.SAEast
    elif inputLocation == 'USWest':
        loc=boto.s3.connection.Location.USWest
    elif inputLocation == 'USWest2':
        loc=boto.s3.connection.Location.USWest2
    
    try:   
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts)    
        bucket_name = 'adsassignment1part2'+str(st).replace(" ", "").replace("-", "").replace(":","").replace(".","")
        bucket = conn.create_bucket(bucket_name, location=loc)
        print("bucket created")
        s3 = boto3.client('s3',
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        
        print('s3 client created')
        
        for f in filepaths:
            try:
                s3.upload_file(f, bucket_name,os.path.basename(f),
                Callback=ProgressPercentage(os.path.basename(f)))
                print("File successfully uploaded to S3",f,bucket)
            except Exception as detail:
                print(detail)
                print("File not uploaded")
                exit()
        
    except:
        logging.info("Amazon keys are invalid!!")
        print("Amazon keys are invalid!!")
        exit()

#do not forget to use the variable filepaths
def zipdir(path, ziph, filepaths):
    ziph.write(os.path.join('merged.csv'))
	
class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()
			
# Calling the main function

def main():
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
    
    argLen=len(sys.argv)
    year=''
    accessKey=''
    secretAccessKey=''
    inputLocation=''
    
    #print("Enter the Year you want the analysis for")
    #year = input("Enter year from 2003 to 2017: ")

    for i in range(1,argLen):
        val=sys.argv[i]
        if val.startswith('year='):
            pos=val.index("=")
            year=val[pos+1:len(val)]
            continue
        elif val.startswith('accessKey='):
            pos=val.index("=")
            accessKey=val[pos+1:len(val)]
            continue
        elif val.startswith('secretKey='):
            pos=val.index("=")
            secretAccessKey=val[pos+1:len(val)]
            continue
        elif val.startswith('location='):
            pos=val.index("=")
            inputLocation=val[pos+1:len(val)]
            continue

    print("Year=",year)
    print("Access Key=",accessKey)
    print("Secret Access Key=",secretAccessKey)
    print("Location=",inputLocation)        
        
    logfilename = 'log_Edgar_'+ year + '_' + st + '.txt'
    print(logfilename)
    logging.basicConfig(filename=logfilename, level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.debug('Program Start')
    logging.debug('*************')    
    logging.debug('Calling the initial URL'.format(year))
    
    #generate files
    valid_year(year)
    
    #prepare log file so that it can be uploaded to cloud
    logger = logging.getLogger()
    logger.disabled = True
    
    filepaths = []
    filepaths.append(os.path.join(logfilename))
    filepaths.append(os.path.join('merged.csv'))
    
    logging.info('Compiled csv and log file zipped')
    
    upload_to_s3(accessKey,secretAccessKey,inputLocation,filepaths)
    
    logger.disabled = False

if __name__ == '__main__':
    main()