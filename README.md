# flink-k8s
An example project to show various Flink job cluster deployments on kubernetes, including an implementation of filesystem-based high availability.

# Scripts
## build-image.sh
A script that builds the Flink docker image with our streaming job embedded. This image is used for both job manager and task manager.

## minikube-build-image.sh 
Same script as `build-image.sh`, but uses minikube's docker to build the image.  
This allows to run the deployments on a local minikube cluster.

## deploy-ha.sh
Deploys a flink job cluster in a high-avalability setup. To configure HA, edit the configmap with your desired HA.
[Link to relevant blog post](https://blog.ronlut.com/flink-job-cluster-on-kubernetes/).

## deploy-fsha-local.sh
Deploys a flink job cluster using the filesystem high availability implemented in this repo.
This uses a shared persistent volume on the local host for HA as well as state storage.
[Link to relevant blog post](https://blog.ronlut.com/flink-job-cluster-on-kubernetes-file-based-high-availability/).

## deploy-fsha-aws-ebs.sh
Deploys a flink job cluster using the filesystem high availability implemented in this repo.
This uses an EBS persistent volume for HA as well as state storage.
[Link to relevant blog post](https://blog.ronlut.com/flink-job-cluster-on-kubernetes-file-based-high-availability/).

---

Questions and PRs are welcome :)
