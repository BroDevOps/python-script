#!/usr/bin/env python3
"""
S3 Upload Script
Uploads JSON files (created by download script) to target S3 bucket
"""

import boto3
import json
import os
import sys
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError
import logging

# =============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# =============================================================================

# AWS Profile for target account
AWS_PROFILE = "adda-prod"  # UPDATE THIS

# Target S3 Bucket Name
BUCKET_NAME = "mock-interview-resumes-prod"  # UPDATE THIS

# Local directory containing JSON files to upload
LOCAL_DATA_DIR = "/Users/adda247/sagar/data-migration"  # UPDATE THIS IF NEEDED

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging():
    """Setup logging configuration"""
    log_filename = f"s3_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

# =============================================================================
# S3 CLIENT SETUP
# =============================================================================

def get_s3_client(profile_name):
    """
    Create S3 client using specified AWS profile
    
    Args:
        profile_name (str): AWS profile name
        
    Returns:
        boto3.client: S3 client object
    """
    try:
        session = boto3.Session(profile_name=profile_name)
        s3_client = session.client('s3')
        
        # Test the connection
        s3_client.list_buckets()
        logger.info(f"✅ Successfully connected to AWS using profile: {profile_name}")
        return s3_client
        
    except NoCredentialsError:
        logger.error(f"❌ No credentials found for profile: {profile_name}")
        raise
    except ClientError as e:
        logger.error(f"❌ Failed to connect with profile {profile_name}: {e}")
        raise

def create_bucket_if_not_exists(s3_client, bucket_name):
    """
    Create S3 bucket if it doesn't exist
    
    Args:
        s3_client: S3 client object
        bucket_name (str): Bucket name to create
    """
    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"✅ Bucket '{bucket_name}' already exists")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == '404':
            # Bucket doesn't exist, create it
            try:
                logger.info(f"🪣 Creating bucket: {bucket_name}")
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"✅ Successfully created bucket: {bucket_name}")
            except ClientError as create_error:
                logger.error(f"❌ Failed to create bucket {bucket_name}: {create_error}")
                raise
        else:
            logger.error(f"❌ Error checking bucket {bucket_name}: {e}")
            raise

# =============================================================================
# UPLOAD FUNCTIONS
# =============================================================================

def find_json_files(directory):
    """
    Find all JSON files in the directory
    
    Args:
        directory (str): Directory to search
        
    Returns:
        list: List of JSON file paths
    """
    json_files = []
    
    if not os.path.exists(directory):
        logger.error(f"❌ Directory does not exist: {directory}")
        return json_files
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    
    logger.info(f"📄 Found {len(json_files)} JSON files in directory: {directory}")
    return json_files

def upload_json_to_s3(s3_client, json_file_path, target_bucket):
    """
    Upload a JSON file to S3, restoring original object
    
    Args:
        s3_client: S3 client object
        json_file_path (str): Path to JSON file
        target_bucket (str): Target bucket name
        
    Returns:
        tuple: (success: bool, original_key: str)
    """
    try:
        # Load JSON data
        with open(json_file_path, 'r', encoding='utf-8') as f:
            object_data = json.load(f)
        
        # Extract metadata and content
        metadata = object_data.get('metadata', {})
        content = object_data.get('content', '')
        original_key = metadata.get('key', '')
        
        if not original_key:
            logger.error(f"❌ No original key found in JSON file: {json_file_path}")
            return False, ""
        
        # Restore content based on encoding
        encoding = metadata.get('encoding', 'utf-8')
        
        if encoding == 'base64':
            import base64
            content_bytes = base64.b64decode(content)
        elif encoding == 'hex':
            content_bytes = bytes.fromhex(content)
        else:
            # Default to UTF-8 encoding
            if isinstance(content, str):
                content_bytes = content.encode('utf-8')
            else:
                content_bytes = str(content).encode('utf-8')
        
        # Prepare upload arguments
        upload_args = {
            'Bucket': target_bucket,
            'Key': original_key,
            'Body': content_bytes
        }
        
        # Add content type if available
        if metadata.get('content_type') and metadata['content_type'] != 'unknown':
            upload_args['ContentType'] = metadata['content_type']
        
        # Upload to S3
        s3_client.put_object(**upload_args)
        
        file_size = len(content_bytes)
        logger.info(f"⬆️  Uploaded: {original_key} ({file_size} bytes)")
        return True, original_key
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON file {json_file_path}: {e}")
        return False, ""
    except Exception as e:
        logger.error(f"❌ Failed to upload {json_file_path}: {e}")
        return False, ""

def upload_all_objects(s3_client, target_bucket, local_dir):
    """
    Upload all JSON files to target S3 bucket
    
    Args:
        s3_client: S3 client object
        target_bucket (str): Target bucket name
        local_dir (str): Local directory containing JSON files
        
    Returns:
        dict: Upload statistics
    """
    logger.info(f"🚀 Starting upload to bucket: {target_bucket}")
    
    # Find all JSON files
    json_files = find_json_files(local_dir)
    
    if not json_files:
        logger.warning(f"⚠️  No JSON files found in directory: {local_dir}")
        return {"total": 0, "successful": 0, "failed": 0, "uploaded_keys": [], "failed_files": []}
    
    successful_uploads = 0
    failed_uploads = 0
    uploaded_keys = []
    failed_files = []
    
    logger.info(f"📤 Starting to upload {len(json_files)} files...")
    
    for i, json_file in enumerate(json_files, 1):
        try:
            filename = os.path.basename(json_file)
            logger.info(f"📂 [{i}/{len(json_files)}] Processing: {filename}")
            
            success, original_key = upload_json_to_s3(s3_client, json_file, target_bucket)
            
            if success:
                successful_uploads += 1
                uploaded_keys.append(original_key)
            else:
                failed_uploads += 1
                failed_files.append(json_file)
                
        except Exception as e:
            logger.error(f"❌ Failed to process {json_file}: {e}")
            failed_uploads += 1
            failed_files.append(json_file)
            continue
    
    stats = {
        "total": len(json_files),
        "successful": successful_uploads,
        "failed": failed_uploads,
        "uploaded_keys": uploaded_keys,
        "failed_files": failed_files
    }
    
    logger.info(f"📊 Upload completed: {successful_uploads} successful, {failed_uploads} failed")
    
    if failed_files:
        logger.warning(f"⚠️  Failed uploads: {[os.path.basename(f) for f in failed_files]}")
    
    return stats

# =============================================================================
# VERIFICATION FUNCTIONS
# =============================================================================

def verify_uploads(s3_client, bucket_name, uploaded_keys):
    """
    Verify that uploaded objects exist in S3
    
    Args:
        s3_client: S3 client object
        bucket_name (str): Bucket name
        uploaded_keys (list): List of uploaded object keys
        
    Returns:
        dict: Verification results
    """
    logger.info(f"🔍 Verifying {len(uploaded_keys)} uploaded objects...")
    
    verified = 0
    missing = 0
    missing_keys = []
    
    for i, key in enumerate(uploaded_keys, 1):
        try:
            if i % 10 == 0 or i == len(uploaded_keys):
                logger.info(f"🔍 Verifying [{i}/{len(uploaded_keys)}]...")
            
            s3_client.head_object(Bucket=bucket_name, Key=key)
            verified += 1
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                missing += 1
                missing_keys.append(key)
                logger.warning(f"⚠️  Missing object: {key}")
            else:
                logger.error(f"❌ Error verifying {key}: {e}")
                missing += 1
                missing_keys.append(key)
    
    verification_stats = {
        "total": len(uploaded_keys),
        "verified": verified,
        "missing": missing,
        "missing_keys": missing_keys
    }
    
    logger.info(f"✅ Verification completed: {verified} verified, {missing} missing")
    return verification_stats

# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    """Main upload function"""
    
    logger.info("="*70)
    logger.info("🔼 S3 UPLOAD SCRIPT STARTED")
    logger.info("="*70)
    
    logger.info(f"📋 Configuration:")
    logger.info(f"   AWS Profile: {AWS_PROFILE}")
    logger.info(f"   Target Bucket: {BUCKET_NAME}")
    logger.info(f"   Local Directory: {LOCAL_DATA_DIR}")
    logger.info("-"*50)
    
    try:
        # Setup S3 client
        logger.info("🔧 Setting up S3 client...")
        s3_client = get_s3_client(AWS_PROFILE)
        
        # Create bucket if needed
        logger.info("🪣 Checking/creating target bucket...")
        create_bucket_if_not_exists(s3_client, BUCKET_NAME)
        
        # Upload all objects
        logger.info("⬆️  Starting upload process...")
        upload_stats = upload_all_objects(s3_client, BUCKET_NAME, LOCAL_DATA_DIR)
        
        # Verify uploads if any were successful
        verification_stats = None
        if upload_stats['successful'] > 0:
            logger.info("🔍 Starting verification process...")
            verification_stats = verify_uploads(s3_client, BUCKET_NAME, upload_stats['uploaded_keys'])
        
        # Print summary
        logger.info("\n" + "="*70)
        logger.info("📊 UPLOAD SUMMARY")
        logger.info("="*70)
        logger.info(f"📂 Total JSON files found: {upload_stats['total']}")
        logger.info(f"✅ Successfully uploaded: {upload_stats['successful']}")
        logger.info(f"❌ Failed uploads: {upload_stats['failed']}")
        logger.info(f"🪣 Target bucket: {BUCKET_NAME}")
        
        if verification_stats:
            logger.info(f"🔍 Objects verified in S3: {verification_stats['verified']}")
            if verification_stats['missing'] > 0:
                logger.warning(f"⚠️  Objects missing after upload: {verification_stats['missing']}")
        
        if upload_stats['successful'] > 0:
            logger.info("🎉 Upload completed successfully!")
            
            # Show some example uploaded keys
            if upload_stats['uploaded_keys']:
                logger.info(f"📄 Example uploaded objects:")
                for key in upload_stats['uploaded_keys'][:5]:  # Show first 5 keys
                    logger.info(f"   - {key}")
                if len(upload_stats['uploaded_keys']) > 5:
                    logger.info(f"   ... and {len(upload_stats['uploaded_keys']) - 5} more objects")
        else:
            logger.error("❌ No files uploaded successfully")
            
        if upload_stats['failed'] > 0:
            logger.warning(f"⚠️  {upload_stats['failed']} files failed to upload")
            
    except Exception as e:
        logger.error(f"💥 Upload process failed: {e}")
        sys.exit(1)

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Setup logging
    logger = setup_logging()
    
    try:
        # Print configuration for confirmation
        print("🔼 S3 Upload Script")
        print("=" * 50)
        print(f"AWS Profile: {AWS_PROFILE}")
        print(f"Target Bucket: {BUCKET_NAME}")
        print(f"Local Directory: {LOCAL_DATA_DIR}")
        print("=" * 50)
        
        # Check if local directory exists
        if not os.path.exists(LOCAL_DATA_DIR):
            print(f"❌ Error: Local directory '{LOCAL_DATA_DIR}' does not exist!")
            print("Please run the download script first or update the LOCAL_DATA_DIR path.")
            sys.exit(1)
        
        # Ask for confirmation
        confirm = input("Do you want to proceed with the upload? (yes/no): ").lower()
        if confirm not in ['yes', 'y']:
            print("❌ Upload cancelled.")
            sys.exit(0)
        
        # Run upload
        main()
        
    except KeyboardInterrupt:
        logger.info("\n⏹️  Upload interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        sys.exit(1)
