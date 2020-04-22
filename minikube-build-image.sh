eval $(minikube docker-env)
base_dir=$(pwd)
cd empty_flink_job
mvn package
cd ..
cd build-image
./build.sh --from-release --flink-version 1.9.2 --scala-version 2.12 --job-artifacts "$base_dir/empty_flink_job/target/empty_flink_job-1.0-SNAPSHOT.jar" --image-name flink-k8s-example