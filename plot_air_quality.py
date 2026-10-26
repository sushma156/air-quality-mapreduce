import matplotlib.pyplot as plt

cities = ["Bengaluru", "Chennai", "Hyderabad"]
averages = [35.0, 45.0, 30.0]

plt.bar(cities, averages)
plt.title("Average PM2.5 by City")
plt.xlabel("City")
plt.ylabel("Average PM2.5 (µg/m³)")
plt.tight_layout()

plt.savefig("average_pm25.png", dpi=300)
plt.show()
