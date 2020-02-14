# Dockerfile for Edgar Dataset Web Scraping

FROM python:3.6-onbuild

# Installing Dependencies
RUN pip install pip
RUN pip install urllib3
#RUN pip install csv
#RUN pip install sys
#RUN pip install os
#RUN pip install re
#RUN pip install zipfile
#RUN pip install logging
#RUN pip install time
#RUN pip install datetime
RUN pip install boto
RUN pip install boto3
#RUN pip install random
RUN pip install bs4

# Copying the python file from our local to the Docker File System
COPY EdgarDataset-WebScraping.py /src/EdgarDataset-WebScraping.py
COPY requirements.txt /src/requirements.txt

#Run the command
CMD ["python", "./EdgarDataset-WebScraping.py"]
