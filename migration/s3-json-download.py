#!/usr/bin/env python3
"""
S3 Download Script
Downloads all objects from S3 bucket and saves them as JSON files
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

# AWS Profile for source account
AWS_PROFILE = "aiml-devo"  # UPDATE THIS

# Source S3 Bucket Name
BUCKET_NAME = "mock-interview-resumes"  # UPDATE THIS

# Local directory for downloaded data
LOCAL_DOWNLOAD_DIR = "/Users/adda247/sagar/data-migration"  # UPDATE THIS IF NEEDED

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging():
    """Setup logging configuration"""
    log_filename = f"s3_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
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

# =============================================================================
# DOWNLOAD FUNCTIONS
# =============================================================================

def list_all_objects(s3_client, bucket_name):
    """
    List all objects in the S3 bucket
    
    Args:
        s3_client: S3 client object
        bucket_name (str): Name of the S3 bucket
        
    Returns:
        list: List of object keys
    """
    logger.info(f"📋 Listing objects in bucket: {bucket_name}")
    
    objects = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    try:
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects.append(obj['Key'])
                    
        logger.info(f"📊 Found {len(objects)} objects in bucket: {bucket_name}")
        return objects
        
    except ClientError as e:
        logger.error(f"❌ Failed to list objects in bucket {bucket_name}: {e}")
        raise

def download_object_as_json(s3_client, bucket_name, object_key, local_dir):
    """
    Download an object and save it as JSON
    
    Args:
        s3_client: S3 client object
        bucket_name (str): Source bucket name
        object_key (str): Object key to download
        local_dir (str): Local directory to save the file
        
    Returns:
        str: Local file path
    """
    try:
        # Create local directory structure
        local_file_path = os.path.join(local_dir, object_key)
        local_file_dir = os.path.dirname(local_file_path)
        os.makedirs(local_file_dir, exist_ok=True)
        
        # Get object metadata
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        
        # Download object
        obj_response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        content = obj_response['Body'].read()
        
        # Prepare JSON structure
        object_data = {
            "metadata": {
                "bucket": bucket_name,
                "key": object_key,
                "size": response.get('ContentLength', 0),
                "last_modified": response.get('LastModified').isoformat() if response.get('LastModified') else None,
                "content_type": response.get('ContentType', 'unknown'),
                "etag": response.get('ETag', '').strip('"'),
                "download_timestamp": datetime.now().isoformat(),
                "aws_profile_used": AWS_PROFILE
            },
            "content": None
        }
        
        # Handle content based on type
        content_type = response.get('ContentType', '').lower()
        
        if content_type.startswith('text/') or 'json' in content_type or content_type == 'application/json':
            try:
                object_data["content"] = content.decode('utf-8')
                object_data["metadata"]["encoding"] = "utf-8"
            except UnicodeDecodeError:
                # If can't decode as UTF-8, store as hex
                object_data["content"] = content.hex()
                object_data["metadata"]["encoding"] = "hex"
        else:
            # For binary files, store as base64
            import base64
            object_data["content"] = base64.b64encode(content).decode('utf-8')
            object_data["metadata"]["encoding"] = "base64"
        
        # Create safe filename for JSON
        safe_filename = object_key.replace('/', '_').replace('\\', '_')
        json_file_path = os.path.join(local_dir, f"{safe_filename}.json")
        
        # Save as JSON
        with open(json_file_path, 'w', encoding='utf-8') as f:
            json.dump(object_data, f, indent=2, ensure_ascii=False)
        
        file_size = os.path.getsize(json_file_path)
        logger.info(f"⬇️  Downloaded: {object_key} -> {json_file_path} ({file_size} bytes)")
        return json_file_path
        
    except ClientError as e:
        logger.error(f"❌ Failed to download {object_key}: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error downloading {object_key}: {e}")
        raise

def download_all_objects(s3_client, bucket_name, local_dir):
    """
    Download all objects from S3 bucket as JSON files
    
    Args:
        s3_client: S3 client object
        bucket_name (str): Source bucket name
        local_dir (str): Local directory to save files
        
    Returns:
        dict: Download statistics
    """
    logger.info(f"🚀 Starting download from bucket: {bucket_name}")
    
    # Create local directory
    os.makedirs(local_dir, exist_ok=True)
    
    # Get list of objects
    objects = list_all_objects(s3_client, bucket_name)
    
    if not objects:
        logger.warning(f"⚠️  No objects found in bucket: {bucket_name}")
        return {"total": 0, "successful": 0, "failed": 0, "failed_objects": []}
    
    downloaded_files = []
    failed_downloads = []
    
    logger.info(f"📥 Starting to download {len(objects)} objects...")
    
    for i, object_key in enumerate(objects, 1):
        try:
            logger.info(f"📂 [{i}/{len(objects)}] Downloading: {object_key}")
            local_file_path = download_object_as_json(s3_client, bucket_name, object_key, local_dir)
            downloaded_files.append(local_file_path)
            
        except Exception as e:
            logger.error(f"❌ Failed to download {object_key}: {e}")
            failed_downloads.append(object_key)
            continue
    
    stats = {
        "total": len(objects),
        "successful": len(downloaded_files),
        "failed": len(failed_downloads),
        "failed_objects": failed_downloads
    }
    
    logger.info(f"📊 Download completed: {stats['successful']} successful, {stats['failed']} failed")
    
    if failed_downloads:
        logger.warning(f"⚠️  Failed downloads: {failed_downloads}")
    
    return stats

# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    """Main download function"""
    
    logger.info("="*70)
    logger.info("🔽 S3 DOWNLOAD SCRIPT STARTED")
    logger.info("="*70)
    
    logger.info(f"📋 Configuration:")
    logger.info(f"   AWS Profile: {AWS_PROFILE}")
    logger.info(f"   Source Bucket: {BUCKET_NAME}")
    logger.info(f"   Local Directory: {LOCAL_DOWNLOAD_DIR}")
    logger.info("-"*50)
    
    try:
        # Setup S3 client
        logger.info("🔧 Setting up S3 client...")
        s3_client = get_s3_client(AWS_PROFILE)
        
        # Download all objects
        logger.info("⬇️  Starting download process...")
        download_stats = download_all_objects(s3_client, BUCKET_NAME, LOCAL_DOWNLOAD_DIR)
        
        # Print summary
        logger.info("\n" + "="*70)
        logger.info("📊 DOWNLOAD SUMMARY")
        logger.info("="*70)
        logger.info(f"📂 Total objects in bucket: {download_stats['total']}")
        logger.info(f"✅ Successfully downloaded: {download_stats['successful']}")
        logger.info(f"❌ Failed downloads: {download_stats['failed']}")
        logger.info(f"📁 Local directory: {os.path.abspath(LOCAL_DOWNLOAD_DIR)}")
        
        if download_stats['successful'] > 0:
            logger.info("🎉 Download completed successfully!")
            
            # List some example files
            json_files = [f for f in os.listdir(LOCAL_DOWNLOAD_DIR) if f.endswith('.json')]
            if json_files:
                logger.info(f"📄 Example downloaded files:")
                for file in json_files[:5]:  # Show first 5 files
                    logger.info(f"   - {file}")
                if len(json_files) > 5:
                    logger.info(f"   ... and {len(json_files) - 5} more files")
        else:
            logger.error("❌ No files downloaded successfully")
            
        if download_stats['failed'] > 0:
            logger.warning(f"⚠️  {download_stats['failed']} files failed to download")
            
    except Exception as e:
        logger.error(f"💥 Download process failed: {e}")
        sys.exit(1)

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Setup logging
    logger = setup_logging()
    
    try:
        # Print configuration for confirmation
        print("🔽 S3 Download Script")
        print("=" * 50)
        print(f"AWS Profile: {AWS_PROFILE}")
        print(f"Source Bucket: {BUCKET_NAME}")
        print(f"Local Directory: {LOCAL_DOWNLOAD_DIR}")
        print("=" * 50)
        
        # Ask for confirmation
        confirm = input("Do you want to proceed with the download? (yes/no): ").lower()
        if confirm not in ['yes', 'y']:
            print("❌ Download cancelled.")
            sys.exit(0)
        
        # Run download
        main()
        
    except KeyboardInterrupt:
        logger.info("\n⏹️  Download interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        sys.exit(1)
