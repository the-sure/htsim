#!/usr/bin/env python3
"""
Generate network flows in the specified format
"""

def generate_ecmp_flows():
    """
    Generate background flows
    - 12*16=192 flows
    - Flow size: 5MB = 5,000,000 bytes
    """
    ecmp_flow_pairs = {}
    
    for perm_size in range(12):
        for i in range(16):
            src = 32 + 64*i + perm_size
            dst = 32 + i + (perm_size+1)*64
            ecmp_flow_pairs[src] = dst
    
    return ecmp_flow_pairs

def generate_ar_flows():
    """
    Generate flows
    - 32 flows
    - Flow size: 32MB = 32,000,000 bytes
    """
    ar_flow_pairs = {}
    
    for perm_size in range(1):
        for i in range(32):
            src = i + perm_size*64
            dst = i + (perm_size+1)*64
            ar_flow_pairs[src] = dst
    
    return ar_flow_pairs

def export_flow_file(ecmp_flows, ar_flows, filename):

    total_flows = len(ecmp_flows) + len(ar_flows)
    
    with open(filename, 'w') as f:
        f.write(f"Nodes 4096\n")
        f.write(f"Connections {total_flows}\n")
        
        flow_id = 1
        
        # AR flows - start at time 0, size 32MB
        for src, dst in sorted(ar_flows.items()):
            f.write(f"{src}->{dst} id {flow_id} start 0 size 32000000\n")
            flow_id += 1
        
        flow_id = 1001

        # ECMP flows - start at time 40, size 5MB
        for src, dst in sorted(ecmp_flows.items()):
            f.write(f"{src}->{dst} id {flow_id} start 0 size 5000000\n")
            flow_id += 1

def main():
    ecmp_flows = generate_ecmp_flows()
    ar_flows = generate_ar_flows()
    export_flow_file(ecmp_flows, ar_flows, "datacenter/connection_matrices/mcc_partial_nscc_16flow_5M.cm")

if __name__ == "__main__":
    main()
