import boto3
import csv

# AWS profile
aws_profile = "aws-profile-name"
session = boto3.Session(profile_name=aws_profile)
ec2 = session.client("ec2")

# Fetch instances (running + stopped)
instances = ec2.describe_instances(
    Filters=[{"Name": "instance-state-name", "Values": ["running", "stopped"]}]
)

# CSV file details
csv_filename = f"{aws_profile}_open_ports_report.csv"
excluded_ports = {80, 443}
csv_headers = [
    "Instance Name", "Instance ID", "Instance State", "Launch Date",
    "Security Group Name", "Security Group ID", "Port", "Open To"
]
data = []

# Process instances
for reservation in instances["Reservations"]:
    for instance in reservation["Instances"]:
        instance_id = instance["InstanceId"]
        instance_state = instance["State"]["Name"]  # Running or Stopped
        launch_date = instance["LaunchTime"].strftime("%Y-%m-%d %H:%M:%S")  # Convert to readable format
        instance_name = next((tag["Value"] for tag in instance.get("Tags", []) if tag["Key"] == "Name"), "N/A")

        for sg in instance["SecurityGroups"]:
            sg_id = sg["GroupId"]
            sg_name = sg["GroupName"]

            # Fetch security group details
            sg_details = ec2.describe_security_groups(GroupIds=[sg_id])
            for permission in sg_details["SecurityGroups"][0].get("IpPermissions", []):
                port = permission.get("FromPort")
                
                if port and port not in excluded_ports:
                    # Check IPv4 (0.0.0.0/0)
                    for ip_range in permission.get("IpRanges", []):
                        if ip_range["CidrIp"] == "0.0.0.0/0":
                            data.append([instance_name, instance_id, instance_state, launch_date, sg_name, sg_id, port, "0.0.0.0/0"])

                    # Check IPv6 (::/0)
                    for ipv6_range in permission.get("Ipv6Ranges", []):
                        if ipv6_range["CidrIpv6"] == "::/0":
                            data.append([instance_name, instance_id, instance_state, launch_date, sg_name, sg_id, port, "::/0"])

# Write to CSV
with open(csv_filename, "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(csv_headers)
    writer.writerows(data)

print(f"✅ Report saved as {csv_filename}")
