import boto3
import datetime
import csv
import os

# === CONFIGURABLE PARAMETERS ===
aws_profile = "adda-prod"
days_threshold = 90  # Warn if keys or passwords older than this

# Setup AWS session
print(f"🔄 Using AWS profile: {aws_profile}")
session = boto3.Session(profile_name=aws_profile)
iam = session.client("iam")

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"{aws_profile}_iam_user_audit_{timestamp}.csv"
csv_headers = [
    "User", "Has AdminAccess", "Access Key ID", "Access Key Active", "Last Used Date",
    "Access Key Age (days)", "Password Last Used", "Password Age (days)", "Issues Found"
]

data = []
today = datetime.datetime.now(datetime.timezone.utc)  # timezone-aware

print("🔍 Fetching IAM users...")
users = iam.list_users()["Users"]

def has_admin_access(user_name):
    # Check attached user policies
    attached_user_policies = iam.list_attached_user_policies(UserName=user_name).get('AttachedPolicies', [])
    for policy in attached_user_policies:
        if policy['PolicyName'] == 'AdministratorAccess':
            return True

    # Check groups for admin access
    groups = iam.list_groups_for_user(UserName=user_name).get('Groups', [])
    for group in groups:
        attached_group_policies = iam.list_attached_group_policies(GroupName=group['GroupName']).get('AttachedPolicies', [])
        for policy in attached_group_policies:
            if policy['PolicyName'] == 'AdministratorAccess':
                return True

    return False

for user in users:
    user_name = user["UserName"]
    user_created = user["CreateDate"]
    password_last_used = user.get("PasswordLastUsed")
    password_age = (today - password_last_used).days if password_last_used else "Never"
    has_admin = has_admin_access(user_name)

    access_keys = iam.list_access_keys(UserName=user_name)["AccessKeyMetadata"]

    if not access_keys:
        data.append([
            user_name, has_admin, "N/A", "N/A", "N/A", "N/A",
            password_last_used.strftime("%Y-%m-%d") if password_last_used else "Never",
            password_age, "No access key"
        ])
        continue

    for key in access_keys:
        key_id = key["AccessKeyId"]
        status = key["Status"]
        create_date = key["CreateDate"]
        key_age = (today - create_date).days
        last_used_date = "Never"

        try:
            usage = iam.get_access_key_last_used(AccessKeyId=key_id)
            last_used = usage["AccessKeyLastUsed"].get("LastUsedDate")
            if last_used:
                last_used_date = last_used.strftime("%Y-%m-%d")
        except:
            last_used_date = "Unknown"

        issues = []
        if has_admin:
            issues.append("Has AdminAccess")
        if status == "Active" and last_used_date == "Never":
            issues.append("Active key never used")
        if key_age > days_threshold:
            issues.append(f"Key not rotated in {key_age} days")
        if isinstance(password_age, int) and password_age > days_threshold:
            issues.append(f"Password not rotated in {password_age} days")

        data.append([
            user_name, has_admin, key_id, status, last_used_date, key_age,
            password_last_used.strftime("%Y-%m-%d") if password_last_used else "Never",
            password_age, ", ".join(issues) if issues else "None"
        ])

# Write report
with open(csv_filename, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(csv_headers)
    writer.writerows(data)

print("\n📊 IAM Audit Report:")
print(f"  • IAM users audited: {len(users)}")
print(f"  • Output saved to: {os.path.abspath(csv_filename)}")

