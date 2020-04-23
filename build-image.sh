base_dir=$(pwd)
mvn package
cd build-image
./build.sh --from-release --flink-version 1.9.2 --scala-version 2.12 --job-artifacts "$base_dir/target/flinkjobcluster-1.0-SNAPSHOT.jar" --image-name flink-k8s-example