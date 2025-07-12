import json
import boto3
import pandas as pd
import os
import re
from datetime import datetime

s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')

# Helper functions for masking structured fields
def mask_email(email):
    if not isinstance(email, str) or '@' not in email:
        return email
    name, domain = email.split('@', 1)
    masked_name = name[0] + '***' if len(name) > 1 else '*'
    return masked_name + '@' + domain

def mask_phone(phone):
    digits = re.sub(r'\D', '', str(phone))
    return '***-***-' + digits[-4:] if len(digits) >= 4 else '[REDACTED]'

def mask_address(addr):
    if not isinstance(addr, str):
        return addr
    parts = addr.split(',')
    return '***, ' + ','.join(parts[1:]).strip() if len(parts) > 1 else '[REDACTED ADDRESS]'

def mask_dob(dob):
    try:
        dt = pd.to_datetime(dob, errors='coerce')
        return dt.strftime('****-%m-%d') if not pd.isna(dt) else '[REDACTED DOB]'
    except:
        return '[REDACTED DOB]'

def mask_ip(ip):
    if not isinstance(ip, str):
        return ip
    parts = ip.split('.')
    return '***.***.' + parts[2] + '.' + parts[3] if len(parts) == 4 else '[REDACTED IP]'

def mask_credit_card(cc):
    digits = re.sub(r'\D', '', str(cc))
    return '**** **** **** ' + digits[-4:] if len(digits) >= 4 else '[REDACTED CC]'

def redact_ssn_in_text(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[REDACTED SSN]', text)

# Call Amazon Bedrock for deep text redaction (e.g., notes/comments)
def redact_with_bedrock(text):
    if not isinstance(text, str) or text.strip() == '':
        return text

    prompt = f"""Redact all personally identifiable information (PII) such as names, SSNs, phone numbers, emails, credit card numbers, etc. from this text:

Text: \"\"\"{text}\"\"\"

Redacted Text:"""

    try:
        body = json.dumps({
            "prompt": prompt,
            "max_tokens_to_sample": 5000,
            "temperature": 0,
            "stop_sequences": ["\n\n"]
        })

        response = bedrock.invoke_model(
            modelId='anthropic.claude-v2',
            contentType='application/json',
            accept='application/json',
            body=body
        )

        completion = json.loads(response['body'].read())['completion']
        return completion.strip()

    except Exception as e:
        print(f"[Bedrock error] {e}")
        return text  # Fallback to original

def redact_pii(df):
    for col in df.columns:
        col_lower = col.lower()
        if col_lower == 'password':
            df[col] = '[REDACTED]'
        elif col_lower == 'first_name' or col_lower == 'last_name':
            df[col] = df[col].astype(str).apply(lambda x: x[0] + '***' if len(x) > 0 else x)
        elif col_lower == 'email':
            df[col] = df[col].apply(mask_email)
        elif col_lower == 'phone':
            df[col] = df[col].apply(mask_phone)
        elif col_lower == 'address':
            df[col] = df[col].apply(mask_address)
        elif col_lower == 'dob':
            df[col] = df[col].apply(mask_dob)
        elif col_lower == 'ip_address':
            df[col] = df[col].apply(mask_ip)
        elif col_lower == 'credit_card_number':
            df[col] = df[col].apply(mask_credit_card)
        elif col_lower in ['notes', 'comments']:
            df[col] = df[col].apply(redact_with_bedrock)
        else:
            df[col] = df[col].apply(redact_ssn_in_text)
    return df

def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print(f"Triggered by file: {key}")

        if key.startswith("OUT/"):
            print("Skipping already processed file.")
            return {'statusCode': 200, 'body': json.dumps('Already processed.')}

        if not key.lower().endswith(".csv"):
            print("Skipping non-CSV file.")
            return {'statusCode': 200, 'body': json.dumps('Not a CSV.')}

        download_path = f"/tmp/{os.path.basename(key)}"
        s3.download_file(bucket, key, download_path)
        print(f"Downloaded: {download_path}")

        df = pd.read_csv(download_path)
        print(f"Loaded DataFrame with {len(df)} rows")

        df = redact_pii(df)
        print("Redaction complete")

        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        base_name = os.path.splitext(os.path.basename(key))[0]
        output_filename = f"{base_name}_processed_{timestamp}.csv"
        output_path = f"/tmp/{output_filename}"
        df.to_csv(output_path, index=False)
        print(f"Saved: {output_path}")

        output_key = f'OUT/{output_filename}'
        s3.upload_file(output_path, bucket, output_key)
        print(f"Uploaded to s3://{bucket}/{output_key}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Processed and saved to {output_key}')
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps('Error processing file.')}
