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
            *[f"sensor {i}" for i in range(2, 13)],  # sensor 2 to 12
        ]
        df = self.add_air_quality_indices(df)
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

    def add_air_quality_indices(self,local_df):
        """
        Add air quality indices to the dataframe

        :param local_df: The dataframe to add the indices to

        :return: The dataframe with the indices added
        """

        # European Air Quality Standards
        standards = pd.DataFrame(
            [[20, 125, 40, 40, 10000, 120]], columns=["pm25", "so2", "no2", "pm10", "co", "o3"]
        )

        column_mappings = {
            "co": "current.air_quality.co",
            "no2": "current.air_quality.no2",
            "o3": "current.air_quality.o3",
            "so2": "current.air_quality.so2",
            "pm25": "current.air_quality.pm2_5",
            "pm10": "current.air_quality.pm10",
        }

        def aqi_(column_name: str):
            mapped_name = column_mappings.get(column_name)
            return local_df[mapped_name] / standards[column_name].values[0] * 100

        for column in column_mappings.keys():
            local_df[f"{column}_aqi"] = aqi_(column)

        aqi_columns = [f"{column}_aqi" for column in column_mappings.keys()]
        is_series = type(local_df) == pd.Series
        local_df['aqi'] = local_df[aqi_columns].mean(axis=1 if not is_series else 0)

        return local_df