import googleapiclient.discovery
from oauth2client.client import GoogleCredentials
 
# change the parameters accordingly
project='PROJECT_ID'
job='JOB_ID'
location='REGION'
  
credentials = GoogleCredentials.get_application_default()
dataflow = googleapiclient.discovery.build('dataflow', 'v1b3', credentials=credentials)

state={"requestedState": "JOB_STATE_CANCELLED"}

result = dataflow.projects().locations().jobs().update(
	projectId=project,
	jobId=job,
	location=location,
	body=state
).execute()

