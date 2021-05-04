import requests
from bs4 import BeautifulSoup
import re
import xml.etree.ElementTree as Xet
import pandas as pd
import os

import zipfile
from io import StringIO
import io
response = requests.get('https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100')
xml_contenet = response.content
soup_data = BeautifulSoup(xml_contenet,features="lxml")
zip_file_url = soup_data.find('str',{'name':'download_link'},text=re.compile('(.*)/DLTINS(.*)(.zip)')).text

r = requests.get(zip_file_url, stream =True)
check = zipfile.is_zipfile(io.BytesIO(r.content))
while not check:
    r = requests.get(zip_file_url, stream =True)
    check = zipfile.is_zipfile(io.BytesIO(r.content))
else:
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()
    f_name = z.namelist()[0]


cols = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp", "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy"]
rows = []
  

# Parsing the XML file
xmlparse = Xet.parse(os.path.join(zip_file_url))
root = xmlparse.getroot()
for i in root.findall('BizData'):
    req = i.find('FinInstrmGnlAttrbts').text
    if req:
        print(req)
    Id = i.find("Id").text
    FullNm = i.find("FullNm").text
    ClssfctnTp = i.find("ClssfctnTp").text
    CmmdtyDerivInd = i.find("CmmdtyDerivInd").text
    NtnlCcy = i.find("NtnlCcy").text
    Issr = i.get("Issr")
  
    rows.append({"FinInstrmGnlAttrbts.Id": Id,
                 "FinInstrmGnlAttrbts.FullNm": FullNm,
                 "FinInstrmGnlAttrbts.ClssfctnTp": ClssfctnTp,
                 "FinInstrmGnlAttrbts.CmmdtyDerivInd": CmmdtyDerivInd,
                 "FinInstrmGnlAttrbts.NtnlCcy": NtnlCcy})
  
df = pd.DataFrame(rows, columns=cols)
  
print(df)
# Writing dataframe to csv
df.to_csv('output.csv')