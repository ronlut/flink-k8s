package com.ronlut.flinkjobcluster.filesystemha;

import java.util.concurrent.Executor;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;

import static org.apache.flink.configuration.HighAvailabilityOptions.HA_STORAGE_PATH;
import static org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils.getJobManagerAddress;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class SingleFsHaServicesFactory implements HighAvailabilityServicesFactory {
    @Override
    public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor) throws Exception {
        final Tuple2<String, Integer> hostnamePort = getJobManagerAddress(configuration);

        final String jobManagerRpcUrl = AkkaRpcServiceUtils.getRpcUrl(
                hostnamePort.f0,
                hostnamePort.f1,
                JobMaster.JOB_MANAGER_NAME,
                HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION, // todo: shouldn't be fixed
                configuration);
        final String resourceManagerRpcUrl = AkkaRpcServiceUtils.getRpcUrl(
                hostnamePort.f0,
                hostnamePort.f1,
                ResourceManager.RESOURCE_MANAGER_NAME,
                HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION, // todo: shouldn't be fixed
                configuration);
        final String dispatcherRpcUrl = AkkaRpcServiceUtils.getRpcUrl(
                hostnamePort.f0,
                hostnamePort.f1,
                Dispatcher.DISPATCHER_NAME,
                HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION, // todo: shouldn't be fixed
                configuration);

        final String address = checkNotNull(configuration.getString(RestOptions.ADDRESS),
                "%s must be set",
                RestOptions.ADDRESS.key());
        final int port = configuration.getInteger(RestOptions.PORT);
        final boolean enableSSL = SSLUtils.isRestSSLEnabled(configuration);
        final String protocol = enableSSL ? "https://" : "http://";

        return new SingleFsHaServices(
                resourceManagerRpcUrl,
                dispatcherRpcUrl,
                jobManagerRpcUrl,
                String.format("%s%s:%s", protocol, address, port),
                new Path(configuration.getString(HA_STORAGE_PATH)),
                executor);
    }
}
