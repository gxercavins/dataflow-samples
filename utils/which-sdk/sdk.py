from apiclient.discovery import build
from oauth2client.client import GoogleCredentials
# import json

# change the parameters accordingly
project = 'PROJECT_ID'
location = 'us-central1'
sdk = '2.5.0'
 
credentials = GoogleCredentials.get_application_default()

# list all recent jobs 
dataflow = build('dataflow', 'v1b3', credentials=credentials)
results = dataflow.projects().locations().jobs().list(projectId=project, location=location).execute()

print "Found {0} jobs in location {1} for project {2}".format(len(results['jobs']), location, project)
print "Now checking which ones use SDK {0}...".format(sdk)

job_list = []
 
 # check SDK version for each job
for job in results['jobs']:
  # print json.dumps(job, sort_keys=False, indent=4)
  job_description = dataflow.projects().locations().jobs().get(projectId=project, location=location, jobId=job['id']).execute()
  job_sdk = job_description['environment']['userAgent']['version']
  job_sdk = job_description['jobMetadata']['sdkVersion']['version']
  
  if job_sdk  == sdk:
    job_list.append(job['id'])
 
print "\nResults:"
print "\n".join("{}: {}".format(*k) for k in enumerate(job_list))
