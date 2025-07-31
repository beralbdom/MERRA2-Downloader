import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

brasil = gpd.read_parquet('brasil.parquet')
df = pd.read_csv('MERRA2/Exportado/Brasil_ventos/vento_50m.csv', parse_dates=['Data']).set_index('Data').to_period('M')
df = df.iloc[0].to_frame().T

fig, ax = plt.subplots(figsize = (5, 5))

for (mes, vel) in df.iterrows():
    coords = [eval(col) for col in vel.index]
    lats = [c[0] for c in coords]
    lons = [c[1] for c in coords]
    
    gdf = gpd.GeoDataFrame(
        {'u10m': vel.values},
        geometry = gpd.points_from_xy(lons, lats),
    )

gdf.plot(ax = ax, column = 'u10m', cmap = 'RdYlGn', legend = False, markersize = 8, marker = 's', vmin = df.values.min(), vmax = df.values.max())
brasil.plot(ax = ax, edgecolor = '#151515', lw = 1, legend = False, facecolor = "#ffffff00")
ax.title.set_text(f'{mes}')
ax.set_xlabel('Longitude [°]')
ax.set_ylabel('Latitude [°]')

fig.colorbar(
    plt.cm.ScalarMappable(
        cmap = 'RdYlGn', 
        norm = plt.Normalize(vmin = df.values.min(), vmax = df.values.max())
    ), ax = ax, fraction = 0.0173, pad = 0.04, aspect = 50
)

plt.tight_layout()
plt.show()