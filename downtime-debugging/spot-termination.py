import boto3
from datetime import datetime, timedelta, timezone
import pytz
import json

# Configuration
region = "ap-south-1"
profile = "adda-prod"
LOOKBACK_MINUTES = 180  # How far back to check terminations

# AWS session and clients
session = boto3.Session(profile_name=profile, region_name=region)
ec2 = session.client('ec2')
sts = session.client('sts')
account_id = sts.get_caller_identity()['Account']

# Get all spot instance requests
def get_spot_instance_requests():
    response = ec2.describe_spot_instance_requests()
    return response['SpotInstanceRequests']

# Get spot price history
def get_spot_price_history(instance_type, az, start_time, end_time):
    response = ec2.describe_spot_price_history(
        InstanceTypes=[instance_type],
        AvailabilityZone=az,
        StartTime=start_time,
        EndTime=end_time,
        ProductDescriptions=['Linux/UNIX'],
        MaxResults=100
    )
    return sorted(response['SpotPriceHistory'], key=lambda x: x['Timestamp'])

# Get network performance using instance type
def get_network_performance(instance_type):
    try:
        response = ec2.describe_instance_types(InstanceTypes=[instance_type])
        net_info = response['InstanceTypes'][0]['NetworkInfo']
        return net_info['NetworkPerformance'], net_info.get('DefaultNetworkCardIndex')
    except Exception:
        return None, None

# Get CPU credit type for burstable instances
def get_cpu_credit_type(instance_type):
    try:
        response = ec2.describe_instance_types(InstanceTypes=[instance_type])
        credit_spec = response['InstanceTypes'][0].get('CpuInfo', {})
        return response['InstanceTypes'][0].get('SupportedUsageClasses', ['standard'])[0]
    except Exception:
        return 'unknown'

# Check spot pool availability
def get_pool_utilization_class(instance_type, az):
    try:
        offerings = ec2.describe_instance_type_offerings(
            LocationType='availability-zone',
            Filters=[
                {'Name': 'instance-type', 'Values': [instance_type]},
                {'Name': 'location', 'Values': [az]}
            ]
        )
        return "available" if offerings['InstanceTypeOfferings'] else "unavailable"
    except Exception:
        return "unknown"

# Generate a descriptive summary
def generate_description(data):
    # desc = f"Instance {data['InstanceId']} of type {data['InstanceType']} was terminated due to: {data['StatusMessage']}. "
    # desc += f"The instance was launched in availability zone {data['AvailabilityZone']} and had a network performance of '{data['NetworkPerformance']}'. "

    # if data['CpuCreditsType'] != 'unknown':
    #     desc += f"It used a CPU credit type of '{data['CpuCreditsType']}', indicating its performance characteristics. "

    # desc += f"The spot price remained stable, starting at {data['SpotPriceAtLaunch']} and ending at {data['SpotPriceAtTermination']} (trend: {data['SpotPriceTrend']}). "
    # if data['PriceVariance'] == 0:
    #     desc += "There was no price fluctuation before termination. "
    # else:
    #     desc += f"Price fluctuated with a variance of {data['PriceVariance']}. "

    # desc += f"The instance ran for {data['UptimeMinutes']} minutes, classified as '{data['UptimeClass']}'. "
    # desc += f"During the launch window, {data['InstancePopularity']} similar spot requests were made, showing high demand. "
    # desc += f"The region saw {data['RegionDemandScore']} terminations recently, and the AZ had {data['AZInterruptionRate']} interruptions, indicating potential capacity pressure. "
    # desc += f"The spot request type was '{data['SpotRequestType']}'. "
    # return desc
    
    desc = (
        f"Instance {data['InstanceId']} (type {data['InstanceType']}) was terminated because there was no available spot capacity in zone {data['AvailabilityZone']}. "
        f"It had network performance of '{data['NetworkPerformance']}' and used '{data['CpuCreditsType']}' CPU credits. "
        f"The spot price remained stable at {data['SpotPriceAtLaunch']} USD. "
        f"The instance lived for {data['UptimeMinutes']} minutes ('{data['UptimeClass']}' uptime). "
        f"You launched {data['InstancePopularity']} similar spot instances in the past hour, indicating high demand in your account. "
        f"In the past {LOOKBACK_MINUTES} minutes, your account had {data['RegionDemandScore']} spot terminations in region {data['Region']}, with {data['AZInterruptionRate']} in AZ {data['AvailabilityZone']}, showing capacity pressure. "
        f"This was a '{data['SpotRequestType']}' spot request, meaning AWS did not retry after termination."
    )
    return desc

def get_az_id_mapping():
    response = ec2.describe_availability_zones(Filters=[{"Name": "region-name", "Values": [region]}])
    return {az['ZoneName']: az['ZoneId'] for az in response['AvailabilityZones']}


def enrich_termination_data(spot_request, all_requests, region_demand_score, az_demand_score):
    instance_id = spot_request.get('InstanceId')
    instance_type = spot_request['LaunchSpecification']['InstanceType']
    az = spot_request.get('LaunchedAvailabilityZone')
    
    az_id_mapping = get_az_id_mapping()
    az_id = az_id_mapping.get(az)
    spot_price = float(spot_request['SpotPrice'])
    state = spot_request['State']
    status_code = spot_request['Status']['Code']
    status_msg = spot_request['Status']['Message']
    create_time = spot_request['CreateTime']
    update_time = spot_request['Status']['UpdateTime']
    request_type = spot_request.get('Type', 'unknown')
    vpc_id = spot_request['LaunchSpecification'].get('VpcId')
    subnet_id = spot_request['LaunchSpecification'].get('SubnetId')

    termination_time = update_time
    local_tz = pytz.timezone('Asia/Kolkata')
    local_time = termination_time.astimezone(local_tz)

    # Time features
    day_of_week = local_time.strftime('%A')
    hour_of_day = local_time.hour
    is_weekend = day_of_week in ['Saturday', 'Sunday']

    # Spot price history
    price_start = termination_time - timedelta(hours=6)
    price_end = termination_time
    price_history = get_spot_price_history(instance_type, az, price_start, price_end)
    prices = [float(p['SpotPrice']) for p in price_history]

    spot_price_at_termination = float(prices[-1]) if prices else spot_price
    spot_price_at_launch = float(prices[0]) if prices else spot_price
    price_trend = spot_price_at_termination - spot_price_at_launch
    price_variance = round((sum((p - spot_price_at_launch)**2 for p in prices) / len(prices))**0.5, 6) if prices else 0.0

    # Derived metrics
    uptime_minutes = round((termination_time - create_time).total_seconds() / 60, 2)
    is_short_lived = uptime_minutes < 60
    uptime_class = "VeryShort" if uptime_minutes < 30 else "Short" if uptime_minutes < 120 else "Long"

    # Instance popularity
    cutoff_time = create_time - timedelta(hours=1)
    count_same_type = sum(1 for req in all_requests 
                          if req['LaunchSpecification']['InstanceType'] == instance_type and
                          req['CreateTime'] >= cutoff_time)

    # Network and CPU info
    network_perf, net_card_index = get_network_performance(instance_type)
    cpu_credit_type = get_cpu_credit_type(instance_type)
    pool_utilization = get_pool_utilization_class(instance_type, az)

    enriched = {
        "InstanceId": instance_id,
        "SpotInstanceRequestId": spot_request['SpotInstanceRequestId'],
        "InstanceType": instance_type,
        "AvailabilityZone": az,
        "AvailabilityZoneId": az_id,
        "SpotPrice": spot_price,
        "SpotPriceAtLaunch": spot_price_at_launch,
        "SpotPriceAtTermination": spot_price_at_termination,
        "SpotPriceTrend": round(price_trend, 6),
        "PriceVariance": price_variance,
        "CreateTime": create_time.isoformat(),
        "TerminationTime": termination_time.isoformat(),
        "DayOfWeek": day_of_week,
        "HourOfDay": hour_of_day,
        "IsWeekend": is_weekend,
        "UptimeMinutes": uptime_minutes,
        "IsShortLived": is_short_lived,
        "UptimeClass": uptime_class,
        "State": state,
        "StatusCode": status_code,
        "StatusMessage": status_msg,
        "InstancePopularity": count_same_type,
        "NetworkPerformance": network_perf,
        "NetworkCardIndex": net_card_index,
        "CpuCreditsType": cpu_credit_type,
        "PoolUtilizationClass": pool_utilization,
        "RegionDemandScore": region_demand_score,
        "AZInterruptionRate": az_demand_score,
        "SpotRequestType": request_type,
        "VpcId": vpc_id,
        "SubnetId": subnet_id,
        "AccountId": account_id,
        "Region": region,
    }

    enriched["Description"] = generate_description(enriched)
    return enriched


spot_requests = get_spot_instance_requests()
now = datetime.now(timezone.utc)
cutoff_time = now - timedelta(minutes=LOOKBACK_MINUTES)

# Filter terminations in last LOOKBACK_MINUTES
recent_terminated = [
    req for req in spot_requests
    if 'terminated' in req['Status']['Code'] and req['Status']['UpdateTime'] >= cutoff_time
]

# Region demand score
region_demand_score = len(recent_terminated)

# AZ interruption rates
az_counter = {}
for req in recent_terminated:
    az = req.get('LaunchedAvailabilityZone')
    az_counter[az] = az_counter.get(az, 0) + 1

# Enrich all terminations
termination_logs = []
for req in recent_terminated:
    az = req.get('LaunchedAvailabilityZone')
    az_demand_score = az_counter.get(az, 0)
    enriched_data = enrich_termination_data(req, spot_requests, region_demand_score, az_demand_score)
    termination_logs.append(enriched_data)

# Save to JSON
with open("spot_termination_data.json", "w") as f:
    json.dump(termination_logs, f, indent=2, default=str)

print(f"Saved {len(termination_logs)} records to spot_termination_data.json")
