package com.edi.sample_prometheus_grafana_kafka.watch.protocol;

import java.util.List;

/** Machine facts the hub shows on the workstation detail page. */
public record HostInfo(
        String username,
        String userDomain,
        String osName,
        String osVersion,
        String kernelArch,
        String cpuModel,
        Integer cpuCores,
        Integer cpuLogical,
        Long memTotalBytes,
        Long diskTotalBytes,
        Long diskFreeBytes,
        String diskPath,
        String primaryIp,
        List<String> ipAddresses,
        String location,
        String assignedTo
) {
}
