kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/storage-local.yaml
ls k8s/*-fsha.yaml | xargs -n 1 kubectl apply -f
ls k8s/*-service.yaml | xargs -n 1 kubectl apply -f