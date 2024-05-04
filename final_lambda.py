from datetime import date
import io
import json
import boto3
import pandas as pd

def lambda_handler(event, context):
    # TODO implement
   today_date=str(date.today())
   s3_client=boto3.client('s3')
   s3_resource=boto3.resource('s3')
   print(event)
   print(context)
   message=event[0]['message']
   print(message)

   if message == {}:
      return {}
   
   try:
     resp = s3_client.get_object(Bucket ='airbnb-assignment-project', Key = f'date={today_date}/Airbnb_{today_date}.csv')
     msg = resp['body'].read()
     s = str(msg, 'utf8')
     data = io.StringIO(s)
     df= pd.read_csv(data, index_col='bookingId')
     df.loc[message['bookingId']] = [message['userId'],message['propertyId'],
                                    message['location'],message['startDate'],message['endDate'],
                                    message['price']]
     
     df.to_csv('/tmp/test.csv', encoding='utf8')
     s3_resource.Bucket('airbnb-assignment-project').upload_file('/tmp/test.csv',f'date={today_date}/Airbnb_{today_date}.csv')
     print(df)

   except Exception as e:
      print(str(e))
      df = pd.DataFrame(columns = ['bookingId','userId','propertyId','location','startDate','endDate','price'])

      df = df.set_index(list(df.columns)[0])

      df.loc[message['bookingId']] = [message['userId'],message['propertyId'],
                                    message['location'],message['startDate'],message['endDate'],
                                    message['price']]
     
      df.to_csv('/tmp/test.csv', encoding='utf8')
      s3_resource.Bucket('airbnb-data-store').upload_file('/tmp/test.csv',f'date={today_date}/Airbnb_{today_date}.csv')


  


      
      



      