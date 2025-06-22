import boto3
import csv
import os
import datetime

# AWS profile and session setup
aws_profile = "monitoring"
print(f"🔄 Using AWS profile: {aws_profile}")
session = boto3.Session(profile_name=aws_profile)
ec2 = session.client("ec2")

# CSV output setup
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"{aws_profile}_open_ports_report_{timestamp}.csv"
csv_headers = [
    "Instance Name", "Instance ID", "Instance State", "Launch Date",
    "VPC ID", "Security Group Name", "Security Group ID",
    "Protocol", "Port Range", "Open To", "Description"
]
data = []

# Debug counters
instance_count = 0
sg_count = 0
rule_count = 0

print("🔍 Fetching EC2 instances...")
# Fetch all EC2 instances (running and stopped)
paginator = ec2.get_paginator('describe_instances')
instances_pages = paginator.paginate(
    Filters=[{"Name": "instance-state-name", "Values": ["running", "stopped"]}]
)

# Process each instance
for page in instances_pages:
    for reservation in page.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            instance_count += 1
            instance_id = instance["InstanceId"]
            instance_state = instance["State"]["Name"]
            launch_date = instance["LaunchTime"].strftime("%Y-%m-%d %H:%M:%S")
            
            # Get instance name from tags or mark as N/A
            instance_name = "N/A"
            if "Tags" in instance:
                for tag in instance["Tags"]:
                    if tag["Key"] == "Name":
                        instance_name = tag["Value"]
                        break
                        
            vpc_id = instance.get("VpcId", "N/A")
            
            print(f"  📌 Processing instance: {instance_name} ({instance_id}) - {instance_state}")
            
            # Get security groups attached to this instance
            for sg in instance.get("SecurityGroups", []):
                sg_count += 1
                sg_id = sg["GroupId"]
                sg_name = sg["GroupName"]
                
                print(f"    🔒 Analyzing security group: {sg_name} ({sg_id})")
                
                # Get detailed security group rules
                try:
                    sg_details = ec2.describe_security_groups(GroupIds=[sg_id])
                    
                    # Process inbound rules (IpPermissions)
                    for permission in sg_details["SecurityGroups"][0].get("IpPermissions", []):
                        protocol = permission.get("IpProtocol", "")
                        
                        # Handle port range
                        from_port = permission.get("FromPort")
                        to_port = permission.get("ToPort")
                        
                        # Format port display
                        if protocol == "-1":  # All traffic
                            protocol_display = "All"
                            port_range = "All"
                        elif from_port is None or to_port is None:
                            port_range = "N/A"
                        elif from_port == to_port:
                            port_range = str(from_port)
                        else:
                            port_range = f"{from_port}-{to_port}"
                        
                        # IPv4 ranges
                        for ip_range in permission.get("IpRanges", []):
                            rule_count += 1
                            cidr_ip = ip_range.get("CidrIp", "")
                            description = ip_range.get("Description", "")
                            
                            data.append([
                                instance_name, instance_id, instance_state, launch_date,
                                vpc_id, sg_name, sg_id, protocol, port_range, cidr_ip, description
                            ])
                        
                        # IPv6 ranges
                        for ipv6_range in permission.get("Ipv6Ranges", []):
                            rule_count += 1
                            cidr_ipv6 = ipv6_range.get("CidrIpv6", "")
                            description = ipv6_range.get("Description", "")
                            
                            data.append([
                                instance_name, instance_id, instance_state, launch_date,
                                vpc_id, sg_name, sg_id, protocol, port_range, cidr_ipv6, description
                            ])
                            
                        # Security group references
                        for group_pair in permission.get("UserIdGroupPairs", []):
                            rule_count += 1
                            group_id = group_pair.get("GroupId", "")
                            user_id = group_pair.get("UserId", "")
                            description = group_pair.get("Description", "")
                            
                            # Try to get referenced security group name
                            ref_sg_name = "Unknown SG"
                            try:
                                ref_sg = ec2.describe_security_groups(GroupIds=[group_id])
                                if ref_sg["SecurityGroups"]:
                                    ref_sg_name = ref_sg["SecurityGroups"][0]["GroupName"]
                            except Exception as e:
                                pass
                                
                            data.append([
                                instance_name, instance_id, instance_state, launch_date,
                                vpc_id, sg_name, sg_id, protocol, port_range, 
                                f"sg:{ref_sg_name} ({group_id})", description
                            ])
                            
                except Exception as e:
                    print(f"    ⚠️ Error processing security group {sg_id}: {str(e)}")

# Write results to CSV
with open(csv_filename, "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(csv_headers)
    writer.writerows(data)

print("\n📊 Summary:")
print(f"  • EC2 instances processed: {instance_count}")
print(f"  • Security groups analyzed: {sg_count}")
print(f"  • Security group rules found: {rule_count}")
print(f"  • Rules exported to CSV: {len(data)}")
print(f"\n📄 Report saved to: {os.path.abspath(csv_filename)}")

if len(data) == 0:
    print("\n⚠️ WARNING: No security group rules were found. Possible issues:")
    print("  • The AWS profile may not have the necessary permissions")
    print("  • There might be no EC2 instances in the specified state")
    print("  • The instances might not have any security group rules")
    print("\nTry running with AWS_DEBUG=true for more detailed information:")
    print("AWS_DEBUG=true python improved-port-scanner.py")

