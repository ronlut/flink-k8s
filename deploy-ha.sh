kubectl apply -f k8s/namespace.yaml
ls k8s/*-ha.yaml | xargs -n 1 kubectl apply -f
ls k8s/*-service.yaml | xargs -n 1 kubectl apply -f