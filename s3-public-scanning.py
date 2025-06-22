import boto3
import csv
import os
import datetime
from botocore.exceptions import ClientError

# AWS profile and session setup
aws_profile = "adda-prod"
print(f"🔄 Using AWS profile: {aws_profile}")
session = boto3.Session(profile_name=aws_profile)
s3 = session.client("s3")

# Output CSV setup
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"{aws_profile}_public_s3_buckets_{timestamp}.csv"
csv_headers = ["Bucket Name", "Region", "Public Access Type", "Details"]

data = []

def is_bucket_public(bucket_name):
    # Check block public access settings
    try:
        block_config = s3.get_bucket_policy_status(Bucket=bucket_name)
        is_public = block_config["PolicyStatus"]["IsPublic"]
        if is_public:
            return "Policy", "Bucket policy allows public access"
    except ClientError as e:
        if e.response['Error']['Code'] != "NoSuchBucketPolicy":
            print(f"⚠️ Error getting policy status for {bucket_name}: {e}")
    
    # Check bucket ACL
    try:
        acl = s3.get_bucket_acl(Bucket=bucket_name)
        for grant in acl.get("Grants", []):
            grantee = grant.get("Grantee", {})
            uri = grantee.get("URI", "")
            if "AllUsers" in uri or "AuthenticatedUsers" in uri:
                return "ACL", f"ACL grants access to {uri.split('/')[-1]}"
    except ClientError as e:
        print(f"⚠️ Error getting ACL for {bucket_name}: {e}")
    
    return None, None

# Get all bucket names
print("📦 Fetching all S3 buckets...")
buckets = s3.list_buckets()["Buckets"]

for bucket in buckets:
    bucket_name = bucket["Name"]
    region = "unknown"

    try:
        region = s3.get_bucket_location(Bucket=bucket_name).get("LocationConstraint") or "us-east-1"
    except Exception as e:
        print(f"⚠️ Could not determine region for {bucket_name}: {e}")
    
    public_type, details = is_bucket_public(bucket_name)
    if public_type:
        data.append([bucket_name, region, public_type, details])
        print(f"🌐 Public Bucket Found: {bucket_name} ({public_type})")

# Write results to CSV
with open(csv_filename, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(csv_headers)
    writer.writerows(data)

print("\n📊 S3 Public Access Report:")
print(f"  • Total buckets scanned: {len(buckets)}")
print(f"  • Public buckets found: {len(data)}")
print(f"  • Report saved to: {os.path.abspath(csv_filename)}")

if not data:
    print("✅ No public buckets found.")

