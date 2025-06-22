import boto3
import csv
import os
import datetime

aws_profile = "adda-prod"
print(f"🔄 Using AWS profile: {aws_profile}")
session = boto3.Session(profile_name=aws_profile)
ec2 = session.client("ec2")

timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"{aws_profile}_only_all_traffic_rules_{timestamp}.csv"
csv_headers = [
    "Security Group Name", "Security Group ID", "Attached Resource", "Resource Type", "Open To"
]

data = []

print("🔍 Fetching all security groups...")
sgs = ec2.describe_security_groups()["SecurityGroups"]
enis = ec2.describe_network_interfaces()["NetworkInterfaces"]

# Helper to check what resource a SG is attached to
def find_attachment(sg_id):
    for eni in enis:
        for group in eni.get("Groups", []):
            if group["GroupId"] == sg_id:
                attachment = eni.get("Attachment", {})
                desc = eni.get("Description", "")
                instance_id = attachment.get("InstanceId")
                if instance_id:
                    try:
                        instance = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0]["Instances"][0]
                        name = next((t["Value"] for t in instance.get("Tags", []) if t["Key"] == "Name"), instance_id)
                        return name, "EC2"
                    except:
                        return instance_id, "EC2"
                return desc or eni["NetworkInterfaceId"], eni.get("InterfaceType", "ENI")
    return "Not Attached", "None"

print("🔍 Scanning for All traffic rules to 0.0.0.0/0 or ::/0...")

for sg in sgs:
    sg_id = sg["GroupId"]
    sg_name = sg["GroupName"]
    attached_name, resource_type = find_attachment(sg_id)

    for permission in sg.get("IpPermissions", []):
        protocol = permission.get("IpProtocol", "")
        from_port = permission.get("FromPort")
        to_port = permission.get("ToPort")

        if protocol == "-1" and from_port is None and to_port is None:
            for ip_range in permission.get("IpRanges", []):
                if ip_range.get("CidrIp") == "0.0.0.0/0":
                    data.append([sg_name, sg_id, attached_name, resource_type, "0.0.0.0/0"])
            for ipv6_range in permission.get("Ipv6Ranges", []):
                if ipv6_range.get("CidrIpv6") == "::/0":
                    data.append([sg_name, sg_id, attached_name, resource_type, "::/0"])

# Write the CSV file
with open(csv_filename, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(csv_headers)
    writer.writerows(data)

print("\n📊 Report generated.")
print(f"  • Total matching rules: {len(data)}")
print(f"  • Saved to: {os.path.abspath(csv_filename)}")

if not data:
    print("⚠️ No 'All traffic to the world' rules found.")

