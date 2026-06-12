#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct fluxon_rdma_port_info_c {
    char *device;
    uint32_t port;
    char *port_key;
    char *netdev;
    char *pci_bdf;
    uint64_t pcie_max_bandwidth_mbps;
    int32_t numa_node;
    uint32_t speed_gbps;
    char *driver;
    char *firmware;
    uint32_t link_layer;
    uint32_t port_state;
    uint32_t phys_state;
    uint32_t active_mtu_bytes;
    uint16_t lid;
    uint32_t gid_count;
    uint8_t open_ok;
    uint8_t alloc_pd_ok;
    uint8_t usable;
    char *last_error;
} fluxon_rdma_port_info_c;

typedef struct fluxon_rdma_probe_result_c {
    fluxon_rdma_port_info_c *ports;
    size_t port_count;
    char *probe_error;
    uint32_t verbs_device_count;
    int32_t ibv_get_device_list_device_count_raw;
    uint8_t ibv_get_device_list_returned_null;
    int32_t ibv_get_device_list_errno;
    char *verbs_device_names_csv;
    char *sysfs_infiniband_entries_csv;
    char *dev_infiniband_entries_csv;
    char *env_rdmav_drivers;
    char *env_ibv_drivers;
    char *env_ld_library_path;
} fluxon_rdma_probe_result_c;

int fluxon_rdma_probe_snapshot(fluxon_rdma_probe_result_c *out);
void fluxon_rdma_probe_result_destroy(fluxon_rdma_probe_result_c *out);

#ifdef __cplusplus
}
#endif
