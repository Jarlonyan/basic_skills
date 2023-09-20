


#列出来当前pod
kubectl get pods 

#进入某个pod
kubectl exec -it xxxx(pod_name) -- /bin/bash

#看配置
kubectl describe pod xxxx(pod_name)

#删除某个pod
kubectl delete pod xxx(pod_name)
kubectl get pod | grep key_word | awk '{print $1}' | xargs kubectl delete pod  #批量删除

#修改pod配置，会重启
kubectl edit deploy xxx(pod_name)
在template:
    metadata:
        annotations:
            force-update: "1"

