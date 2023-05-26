FROM apache/nifi:latest



USER root

#COPY ./script /opt/nifi/scripts
#RUN apt install python3.9-minimal
#RUN apt-mark auto python3.9-minimal
RUN apt-get update && apt-get install -y python3 python3-pip
# Install libraries
RUN pip3 install xlrd
RUN pip3 install unicodecsv
RUN pip3 install pandas
RUN pip3 install datetime
RUN pip3 install openpyxl
RUN pip3 install convertdate
RUN pip3 install numpy

CMD tail -f /dev/null
