bash
kube_pod_info{pod="mock-inteview-backend-prod-main-8676b8b459-8hf6s"}
# paste this query into explore page
# out of all the results, copy any on json
"""
{__name__="kube_pod_info", app_kubernetes_io_component="metrics", app_kubernetes_io_instance="prometheus-adda-prod", app_kubernetes_io_managed_by="Helm", app_kubernetes_io_name="kube-state-metrics", app_kubernetes_io_part_of="kube-state-metrics", app_kubernetes_io_version="2.8.0", created_by_kind="ReplicaSet", created_by_name="mock-inteview-backend-prod-main-8676b8b459", helm_sh_chart="kube-state-metrics-4.30.0", host_ip="10.203.70.123", host_network="false", instance="10.203.69.22:8080", job="kubernetes-service-endpoints", namespace="addalabs-production", node="ip-10-203-70-123.ap-south-1.compute.internal", pod="mock-inteview-backend-prod-main-8676b8b459-8hf6s", pod_ip="10.203.70.145", service="prometheus-adda-prod-kube-state-metrics", uid="7643ac01-6cc3-404e-9115-90861914046d"}
"""
# take out the "host_ip" from above ---> host_ip="10.203.70.123"

kube_node_info{internal_ip="10.203.70.123"}
"""
{__name__="kube_node_info", app_kubernetes_io_component="metrics", app_kubernetes_io_instance="prometheus-adda-prod", app_kubernetes_io_managed_by="Helm", app_kubernetes_io_name="kube-state-metrics", app_kubernetes_io_part_of="kube-state-metrics", app_kubernetes_io_version="2.8.0", container_runtime_version="containerd://1.7.27", helm_sh_chart="kube-state-metrics-4.30.0", instance="10.203.69.22:8080", internal_ip="10.203.70.123", job="kubernetes-service-endpoints", kernel_version="5.10.236-228.935.amzn2.x86_64", kubelet_version="v1.32.3-eks-473151a", kubeproxy_version="v1.32.3-eks-473151a", namespace="monitoring", node="ip-10-203-70-123.ap-south-1.compute.internal", os_image="Amazon Linux 2", provider_id="aws:///ap-south-1a/i-0bb1201147d0daa54", service="prometheus-adda-prod-kube-state-metrics", system_uuid="ec209721-ab8d-9c92-9b7e-7cc4e356ced1"}
"""
# now we have recieved the instance id --> i-0bb1201147d0daa54

## run the script first

#### then find the same instance id from the data and check the events


