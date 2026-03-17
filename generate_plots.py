import matplotlib.pyplot as plt
import numpy as np

# 1. Runtime vs Worker Count (Scaling Efficiency)
workers = ['Local\n(1 Worker)', 'Distributed\n(2 Workers)', 'Distributed\n(4 Workers)']
runtimes = [42.5, 24.1, 14.2]

plt.figure(figsize=(8, 5))
bars = plt.bar(workers, runtimes, color=['#ff9999', '#66b3ff', '#99ff99'], edgecolor='black')
plt.title('Pipeline Runtime vs. Worker Count', fontsize=14)
plt.ylabel('Total Runtime (Seconds)', fontsize=12)
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Add data labels on top of bars
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 0.5, f"{yval}s", ha='center', fontweight='bold')

plt.savefig('runtime_vs_workers.png', bbox_inches='tight')
plt.clf()

# 2. Throughput vs Partition Size (Partition Tuning Challenge)
partitions = ['10', '50', '100', '200', '500']
throughput = [120000, 250000, 310000, 280000, 150000] # Rows per second

plt.figure(figsize=(8, 5))
plt.plot(partitions, throughput, marker='o', linestyle='-', color='#800080', linewidth=2, markersize=8)
plt.title('Throughput vs. Shuffle Partition Size', fontsize=14)
plt.xlabel('Number of Partitions (spark.sql.shuffle.partitions)', fontsize=12)
plt.ylabel('Throughput (Rows/Second)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)

# Highlight the optimal point
plt.annotate('Optimal Overlap', xy=(2, 310000), xytext=(2.5, 250000),
             arrowprops=dict(facecolor='black', shrink=0.05), fontsize=10)

plt.savefig('throughput_vs_partitions.png', bbox_inches='tight')
print("Successfully generated runtime_vs_workers.png and throughput_vs_partitions.png!")