import boto3
import csv
import io
import json
import os
import re

def get_bedrock_client():
    return boto3.client(
        'bedrock-runtime',
        region_name='us-east-1'
    )

def read_prompt_template():
    """Read prompt template from file"""
    try:
        # Get the directory where the lambda function code is located
        current_dir = os.path.dirname(os.path.realpath(__file__))
        prompt_file = os.path.join(current_dir, 'prompt_csv.txt')
        
        with open(prompt_file, 'r') as file:
            return file.read()
    except Exception as e:
        print(f"Error reading prompt template: {str(e)}")
        raise

def clean_json_response(response):
    """Extract and clean JSON from the response"""
    try:
        response = response.strip()
        try:
            return json.loads(response)
        except:
            pass
            
        start = response.find('{')
        end = response.rfind('}') + 1
        if start != -1 and end != 0:
            json_str = response[start:end]
            json_str = json_str.replace('\\"', '"').replace('\\n', '\n')
            return json.loads(json_str)
            
        print(f"Could not find valid JSON in response: {response}")
        return None
    except Exception as e:
        print(f"Error cleaning JSON response: {str(e)}")
        print(f"Original response: {response}")
        return None

def process_bedrock_response(response_stream):
    """Process the streaming response from Bedrock"""
    full_response = ""
    try:
        if 'body' not in response_stream:
            print("No 'body' in response stream")
            return full_response
            
        for event in response_stream['body']:
            if not event or 'chunk' not in event:
                continue
                
            try:
                chunk_data = json.loads(event['chunk']['bytes'].decode())
                
                if ('type' in chunk_data and 
                    chunk_data['type'] == 'content_block_delta' and 
                    'delta' in chunk_data and 
                    'text' in chunk_data['delta']['type'] and 
                    'text' in chunk_data['delta']):
                    full_response += chunk_data['delta']['text']
                    
            except Exception as e:
                print(f"Error processing chunk: {str(e)}")
                
    except Exception as e:
        print(f"Error processing response stream: {str(e)}")
    
    return full_response

def lambda_handler(event, context):
    bedrock_runtime = get_bedrock_client()
    s3 = boto3.client('s3')
    
    try:
        # Read the prompt template
        prompt_template = read_prompt_template()
        
        bucket = "datamaskingpoc"
        key = "csv/IN/sample_data1.csv"
        
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        reader = csv.reader(io.StringIO(content))
        headers = next(reader)
        print("CSV Headers:", headers)
        
        # Format the prompt with the headers
        prompt = prompt_template.format(headers=headers)

        bedrock_response = bedrock_runtime.invoke_model_with_response_stream(
            modelId='us.anthropic.claude-3-5-sonnet-20241022-v2:0',
            contentType='application/json',
            accept='application/json',
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 5000,
                "temperature": 0.1,
                "top_p": 1
            })
        )
        
        full_response = process_bedrock_response(bedrock_response)
        
        if not full_response:
            raise Exception("Empty response from Bedrock")
            
        print("Raw response:", full_response)
        
        pii_analysis = clean_json_response(full_response)
        
        if pii_analysis is None:
            pii_analysis = {
                "pii_columns": {
                    "NAME": [],
                    "DOB": [],
                    "PHONE": [],
                    "EMAIL": []
                }
            }
        
        print("PII Analysis:", json.dumps(pii_analysis, indent=2))
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'pii_analysis': pii_analysis
            })
        }
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'raw_response': full_response if 'full_response' in locals() else None
            })
        }
