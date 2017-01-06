import numpy as np
import matplotlib.pyplot as plt

wranglerTTC = (3591.36,1915,1055.0125,700.5633334)
cometTTC = (2754.49,1434.456667,802.0666666,467.6800001)

wrangler_vanilla_ttc =(3474.8219,1813.5771,1136.0585, 804.02)
wrangler_vanilla_adj=(3395.172,1736.1023,1054.026,711.1802)
wrangler_vanilla_comp=(79.6499,77.4748,82.0325,92.8398)

comet_vanilla_ttc=(2003.41526667,1100.03976667,738.929666667,633.636966667)
comet_vanilla_adj=(1933.91086,1032.86576, 673.2798,564.9407)
comet_vanilla_comp=(69.5044,67.174,65.6498666667,68.6962666667)

comet_rows_ttc=(4437.5985,2396.70565,1471.52725,1255.23775)
comet_rows_adj=(4341.6204,2298.220,1375.330,1156.67)
comet_rows_comp=(95.97805,98.4855,96.1965,98.56235)

width = 0.35
width3 = 0.25

fig, ax = plt.subplots()
fig2, ax2 = plt.subplots()

N=4
ind = np.arange(N)

rects1 = ax.bar(1.1*ind, cometTTC, width3, color='r')
rects2 = ax.bar(1.1*ind+width3,comet_vanilla_ttc, width3, color = 'y')
rects3 = ax.bar(1.1*ind+width3,comet_vanilla_adj, width3, color = 'b')
rects4 = ax.bar(1.1*ind+2*width3,comet_rows_ttc, width3, color = 'g')
rects5 = ax.bar(1.1*ind+2*width3,comet_rows_adj, width3, color = 'c')

ax.set_ylabel('Average TTC (sec)')
ax.set_xlabel('Cores')
ax.set_title('Leaflet Finder TTC for 145K atoms data set, Comet, 8/10/2016')
ax.set_xticks(1.1*ind + 1.5*width3)
ax.set_xticklabels(('24','48','96','192'))

ax.legend((rects1[0],rects2[0],rects4[0]),('Spark','Vanilla','Vanilla w/ Row-Partitions'))

rects1w = ax2.bar(ind, wranglerTTC, width, color ='r')
rects2w = ax2.bar(ind+width, wrangler_vanilla_ttc, width, color='y')
rects3w = ax2.bar(ind+width, wrangler_vanilla_adj, width, color = 'b')

ax2.set_ylabel('Average TTC (sec)')
ax2.set_xlabel('Cores')
ax2.set_title('Leaflet Finder TTC for 145K atoms data set, Wrangler, 7/27/2016')
ax2.set_xticks(ind + width)
ax2.set_xticklabels(('48','96','192','384'))

ax2.legend((rects1w[0],rects2w[0],rects3w[0]),('Spark','Vanilla Conn Comp,','Vanilla Adj Matrix'))

plt.show()


