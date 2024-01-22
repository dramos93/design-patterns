import requests
import os

# Set the GitHub repository details
repo_owner = 'cervejaria-ambev'
repo_name = 'people-datapipeline'
file_path = 'datafactory/pipeline/0_Engage_Master_PPL.json'

# Get the GitHub access token from environment variables
access_token = os.getenv('GITHUB_ACCESS_TOKEN')

# Construct the API URL
api_url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{file_path}'

# Set the request headers
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/vnd.github.v3+json'
}

# Send the GET request to the GitHub API
response = requests.get(api_url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    # Get the JSON content from the response
    json_content = response.json()

    # Extract the pipeline definitions from the JSON content
    pipeline_definitions = json_content['content']

    # Decode the base64-encoded content
    import base64
    decoded_content = base64.b64decode(pipeline_definitions).decode('utf-8')

    # Print the pipeline definitions
    print(decoded_content)
else:
    print(f'Failed to retrieve the JSON file. Status code: {response.status_code}')


import pandas as pd
import json
import os
import json

# Create empty DataFrames
activities_df = pd.DataFrame(columns=['name', 'type'])
dependencies_df = pd.DataFrame(columns=['activity', 'dependencyConditions'])
userProperties_df = pd.DataFrame()
typeProperties_df = pd.DataFrame(columns=['pipeline', 'waitOnCompletion'])
pipeline_df = pd.DataFrame(columns=['referenceName', 'type'])
folder_df = pd.DataFrame(columns=['name'])
annotations_df = pd.DataFrame()

# Parse the JSON
data = json.loads(decoded_content)

# Extract the activities
for activity in data['properties']['activities']:
    activities_df = pd.concat([activities_df, pd.DataFrame([{'name': activity['name'], 'type': activity['type']}])], ignore_index=True)

    # Extract the dependencies
    for dependency in activity['dependsOn']:
        dependencies_df = pd.concat([dependencies_df, pd.DataFrame([dependency])], ignore_index=True)

    # Extract the userProperties
    userProperties_df = pd.concat([userProperties_df, pd.json_normalize(activity['userProperties'])], ignore_index=True)

    # Extract the typeProperties
    typeProperties_df = pd.concat([typeProperties_df, pd.DataFrame([activity['typeProperties']])], ignore_index=True)

    # Extract the pipeline
    pipeline_df = pd.concat([pipeline_df, pd.DataFrame([activity['typeProperties']['pipeline']])], ignore_index=True)

# Extract the folder
folder_df = pd.concat([folder_df, pd.DataFrame([data['properties']['folder']])], ignore_index=True)
activities_df = pd.concat([activities_df, pd.DataFrame([{'name': 'activity_name', 'type': 'activity_type'}])], ignore_index=True)
# Extract the annotations
annotations_df = pd.concat([annotations_df, pd.json_normalize(data['properties']['annotations'])], ignore_index=True)

# Print the DataFrames
# Define the output folder path
output_folder = 'data'

# Create the output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Define the output file paths
activities_file = os.path.join(output_folder, 'activities.json')
dependencies_file = os.path.join(output_folder, 'dependencies.json')
userProperties_file = os.path.join(output_folder, 'userProperties.json')
typeProperties_file = os.path.join(output_folder, 'typeProperties.json')
pipeline_file = os.path.join(output_folder, 'pipeline.json')
folder_file = os.path.join(output_folder, 'folder.json')
annotations_file = os.path.join(output_folder, 'annotations.json')

# Save the DataFrames to JSON files
activities_df.to_json(activities_file, orient='records', indent=4)
dependencies_df.to_json(dependencies_file, orient='records', indent=4)
userProperties_df.to_json(userProperties_file, orient='records', indent=4)
typeProperties_df.to_json(typeProperties_file, orient='records', indent=4)
pipeline_df.to_json(pipeline_file, orient='records', indent=4)
folder_df.to_json(folder_file, orient='records', indent=4)
annotations_df.to_json(annotations_file, orient='records', indent=4)
