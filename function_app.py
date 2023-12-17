import json
import logging
import uuid

import azure.functions as func
import pandas as pd

app = func.FunctionApp()


@app.function_name(name="data-distribution-function")
@app.service_bus_queue_trigger(
    arg_name="message",
    queue_name="data-cute",
    connection="AzureServiceBusConnectionString",
)
@app.cosmos_db_output(
    arg_name="doc",
    database_name="serverlessdb",
    container_name="data-readings",
    connection="AzureCosmosDBConnectionString",
    createLeaseCollectionIfNotExists=True,
)
@app.service_bus_topic_output(
    topic_name="clean-data-cute",
    arg_name="msgout",
    queue_name="clean-data-cute",
    connection="AzureServiceBusConnectionString",
)
@app.service_bus_topic_output(
    topic_name="to-predict-cute",
    arg_name="topredict",
    queue_name="to-predict-cute",
    connection="AzureServiceBusConnectionString",
)
def data_distribution_function(
        message: func.ServiceBusMessage,
        msgout: func.Out[str],
        doc: func.Out[func.Document],
        topredict: func.Out[str],
):
    message_body = message.get_body().decode("utf-8")
    logging.info("Received message: %s", message_body)

    transformer = Transformer()
    try:
        series = pd.Series(json.loads(message_body))
        df = pd.DataFrame(series).T
    except Exception as e:
        logging.error(f"Error in converting message to DataFrame: {e}")

    # Log DataFrame structure for debugging
    logging.info("DataFrame structure: %s", df.dtypes)

    try:
        # Transform the DataFrame
        transformed_df = transformer.transform(df)
        logging.info("Transformed DataFrame structure: %s", transformed_df.dtypes)

        dict_data = transformed_df.to_dict(orient="records")
        dict_data[0]["id"] = str(uuid.uuid4())

        # Convert from dict to JSON (by turning it back into a DataFrame)
        df_transformed = pd.DataFrame(dict_data)

        # Backend needs the AQI values, these are calculated model side as well
        backend_json = transformer.add_air_quality_indices(df_transformed)
        backend_json.timestamp = pd.to_datetime(backend_json.timestamp, unit="ms")
        backend_json = backend_json.to_json(orient="records")

        # Prediction engine is just the raw data
        json_data = df_transformed.to_json(orient="records")

        # Set the reading to be saved
        reading = func.Document.from_dict(dict_data[0])

        # Outputting the message to respectively
        # 1. Cosmos DB
        # 2. Backend
        # 3. Prediction Engine

        doc.set(reading)  # Comment this out for dev purposes
        msgout.set(backend_json)  # Comment this out for dev purpose
        topredict.set(json_data)  # Comment this out for dev purpose
        logging.info("Message saved to Cosmos DB")
    except Exception as e:
        logging.error(f"Error in transforming or saving data: {e}")


# UNCOMMENT FOR DEV PURPOSES #
# Save message to a JSON file
# message_data = json.loads(message_body)
# with open("message.json", "a") as outfile:
#     json.dump(message_data, outfile)
#     outfile.write("\n")  # Add a newline for each new message


@app.function_name("test-function-servicebus-data-cleaned-with-ids-30-rows")
@app.route("test-function-servicebus-cleaned-with-ids-limited-rows", methods=["GET"])
@app.service_bus_topic_output(
    topic_name="clean-data-cute",
    arg_name="message",
    queue_name="clean-data-queue-dev",
    connection="AzureServiceBusConnectionString",
)
def test_function(req, message: func.Out[str]):
    df = pd.read_csv("cleaned-with-ids.csv")
    dict_data = df.to_dict(orient="records")
    df_transformed = pd.DataFrame(dict_data)
    transformer = Transformer()
    df_transformed = transformer.add_air_quality_indices(df_transformed)
    df_transformed.timestamp = pd.to_datetime(df_transformed.timestamp)
    df_transformed.timestamp = df_transformed.timestamp.astype("int64") // 10 ** 6

    json_data = df_transformed.to_json(orient="records")
    try:
        message.set(json_data)
        logging.info("Message Sent to queue")
        return func.HttpResponse("Message Sent to queue")
    except Exception as e:
        logging.error(f"Error in transforming or saving data: {e}")
        func.HttpResponse("Error in transforming or saving data: {e}")


@app.function_name("test-function-servicebus-data-cleaned")
@app.route("test-function-servicebus-cleaned", methods=["GET"])
@app.service_bus_topic_output(
    topic_name="clean-data-cute",
    arg_name="message",
    queue_name="clean-data-queue-dev",
    connection="AzureServiceBusConnectionString",
)
def test_function(req, message: func.Out[str]):
    df = pd.read_csv("cleaned.csv")
    dict_data = df.to_dict(orient="records")
    for each in dict_data:
        each["id"] = str(uuid.uuid4())
    df_transformed = pd.DataFrame(dict_data)
    transformer = Transformer()
    df_transformed = transformer.add_air_quality_indices(df_transformed)
    df_transformed.timestamp = pd.to_datetime(df_transformed.timestamp)
    df_transformed.timestamp = df_transformed.timestamp.astype("int64") // 10 ** 6
    json_data = df_transformed.to_json(orient="records")
    try:
        message.set(json_data)
        logging.info("Message Sent to queue")
        return func.HttpResponse("Message Sent to queue")
    except Exception as e:
        logging.error(f"Error in transforming or saving data: {e}")
        func.HttpResponse("Error in transforming or saving data: {e}")

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

        # Iterate each row if it is a dataframe, else iterate each column if it is a series (single row)
        local_df['aqi'] = local_df[aqi_columns].mean(axis=1 if not is_series else 0)

        return local_df