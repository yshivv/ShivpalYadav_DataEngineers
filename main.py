import logging
import boto3
import zipfile
import io
import csv
import xml.etree.ElementTree as ET
from urllib.request import urlretrieve

def lambda_handler(event, context):
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Set up S3 client
    s3 = boto3.client('s3')

    # Define variables
    url = 'http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip'
    file_type = 'DLTINS'

    # Download zip file and extract xml
    logging.info(f'Downloading {url}...')
    zip_file = io.BytesIO()
    urlretrieve(url, zip_file)
    with zipfile.ZipFile(zip_file) as zip_ref:
        xml_file = zip_ref.read(zip_ref.namelist()[0]).decode('utf-8')

    # Parse xml and create CSV
    root = ET.fromstring(xml_file)
    csv_data = [['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp',
                 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']]
    for child in root:
        if child.tag.endswith('FinInstrmGnlAttrbts') and child.attrib.get('CmmdtyDerivInd') == 'N':
            csv_data.append([child.attrib.get('Id'), child.attrib.get('FullNm'), child.attrib.get('ClssfctnTp'),
                             child.attrib.get('CmmdtyDerivInd'), child.attrib.get('NtnlCcy'), child.find('Issr').text])
    logging.info(f'CSV data: {csv_data}')

    # Save CSV to S3
    bucket_name = 'my-bucket'
    key = 'DLTINS.csv'
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    for row in csv_data:
        writer.writerow(row)
    s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
    logging.info(f'CSV saved to s3://{bucket_name}/{key}')
