import json

import pandas as pd


class Transformer:
    """
    Transformer class to transform the data
    """

    def __init__(self):
        pass

    def transform(self, df):
        df = self.explode_weather_data(df)
        df["p1"] = df["sensor 0"].apply(lambda x: pd.Series(x).get("value"))
        df["p2"] = df["sensor 1"].apply(lambda x: pd.Series(x).get("value"))
        columns_to_drop = [
            "sensor 0",
            "sensor 1",
            "current.is_day",
            "current.condition.text",
            "current.condition.icon",
            "current.wind_kph",
            "current.wind_dir",
            "current.pressure_mb",
            "current.precip_mm",
            "current.humidity",
            "current.cloud",
            "current.feelslike_c",
            "current.vis_km",
            "country",
            *[f"sensor {i}" for i in range(2, 13)], # sensor 2 to 12
        ]

        return df.drop(columns=columns_to_drop)

    def convert_string_to_json(self, s):
        s = s.replace("'", '"')
        return json.loads(s)

    def explode_weather_data(self, df):
        weather_data = df["weather_data"].apply(self.convert_string_to_json)
        weather_df = pd.json_normalize(weather_data)
        df = pd.concat([df, weather_df], axis=1)
        df = df.drop(
            [
                "location.name",
                "location.region",
                "location.country",
                "location.lat",
                "location.lon",
                "location.tz_id",
                "location.localtime_epoch",
                "location.localtime",
                "current.last_updated_epoch",
                "current.temp_f",
                "timezone",
                "date",
                "current.feelslike_f",
                "current.vis_miles",
                "current.wind_mph",
                "current.gust_mph",
                "current.condition.code",
                "current.pressure_in",
                "current.precip_in",
                "current.wind_degree",  # very few values
                "weather_data",
            ],
            axis=1,
        )
        return df

    def explode_column(self, df, column):
        exploded = df[column].apply(pd.Series)
        df = pd.concat([df, exploded], axis=1)
        df = df.drop(columns=[column])
        return df
