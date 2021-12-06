docker build -t 192.168.0.1:5000/gpushare-scheduler:v1.0 .
docker push  192.168.0.1:5000/gpushare-scheduler:v1.0
kubectl apply -f deployment.yaml


