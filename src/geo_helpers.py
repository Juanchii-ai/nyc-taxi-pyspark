from __future__ import annotations

import folium
import geopandas as gpd
import pandas as pd


def build_trip_map(districts: gpd.GeoDataFrame, trips: pd.DataFrame) -> folium.Map:
    result = folium.Map(location=[40.73, -73.94], zoom_start=10, tiles="CartoDB positron")
    folium.GeoJson(districts.to_json(), name="Community districts").add_to(result)
    for row in trips.dropna(subset=["pickup_latitude", "pickup_longitude"]).itertuples():
        folium.CircleMarker(
            [row.pickup_latitude, row.pickup_longitude],
            radius=2,
            weight=0,
            fill=True,
            fill_opacity=0.45,
        ).add_to(result)
    folium.LayerControl().add_to(result)
    return result

