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
def test_function(
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

        # normalize the air quality data and add it to the dataframe
        transformed_df['co_norm'] = (transformed_df['current.air_quality.co'] - transformed_df['current.air_quality.co'].min()) / (
                transformed_df['current.air_quality.co'].max() - transformed_df['current.air_quality.co'].min())
        transformed_df['no2_norm'] = (transformed_df['current.air_quality.no2'] - transformed_df['current.air_quality.no2'].min()) / (
                transformed_df['current.air_quality.no2'].max() - transformed_df['current.air_quality.no2'].min())
        transformed_df['o3_norm'] = (transformed_df['current.air_quality.o3'] - transformed_df['current.air_quality.o3'].min()) / (
                transformed_df['current.air_quality.o3'].max() - transformed_df['current.air_quality.o3'].min())
        transformed_df['pm10_norm'] = (transformed_df['current.air_quality.pm10'] - transformed_df['current.air_quality.pm10'].min()) / (
                transformed_df['current.air_quality.pm10'].max() - transformed_df['current.air_quality.pm10'].min())
        transformed_df['pm25_norm'] = (transformed_df['current.air_quality.pm2_5'] - transformed_df['current.air_quality.pm2_5'].min()) / (
                transformed_df['current.air_quality.pm2_5'].max() - transformed_df['current.air_quality.pm2_5'].min())
        transformed_df['so2_norm'] = (transformed_df['current.air_quality.so2'] - transformed_df['current.air_quality.so2'].min()) / (
                transformed_df['current.air_quality.so2'].max() - transformed_df['current.air_quality.so2'].min())
        transformed_df['p1_norm'] = (transformed_df['p1'] - transformed_df['p1'].min()) / (transformed_df['p1'].max() - transformed_df['p1'].min())
        transformed_df['p2_norm'] = (transformed_df['p2'] - transformed_df['p2'].min()) / (transformed_df['p2'].max() - transformed_df['p2'].min())

        transformed_df['air_quality'] = transformed_df[['co_norm', 'no2_norm', 'o3_norm', 'pm10_norm', 'pm25_norm', 'so2_norm']].mean(axis=1)
        transformed_df = df.drop(['co_norm', 'no2_norm', 'o3_norm', 'pm10_norm', 'pm25_norm', 'so2_norm', 'p1_norm', 'p2_norm'],
                     axis=1)

        dict_data = transformed_df.to_dict(orient="records")
        dict_data[0]["id"] = str(uuid.uuid4())

        # Convert from dict to JSON (by turning it back into a DataFrame)
        df_transformed = pd.DataFrame(dict_data)
        json_data = df_transformed.to_json(orient="records")

        # Set the reading to be saved
        reading = func.Document.from_dict(dict_data[0])

        # Outputting the message to respectively
        # 1. Cosmos DB
        # 2. Backend
        # 3. Prediction Engine

        doc.set(reading)  # Comment this out for dev purposes
        msgout.set(json_data)  # Comment this out for dev purpose
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


@app.function_name("test-function-servicebus-data-cleaned")
@app.route("test-function-servicebus-cleaned", methods=["GET"])
@app.service_bus_topic_output(
    topic_name="clean-data-cute",
    arg_name="message",
    queue_name="clean-data-cute",
    connection="AzureServiceBusConnectionString",
)
def test_function(req, message: func.Out[str]):
    df = pd.read_csv("cleaned.csv")

    # normalize the air quality data and add it to the dataframe
    df['co_norm'] = (df['current.air_quality.co'] - df['current.air_quality.co'].min()) / (
                df['current.air_quality.co'].max() - df['current.air_quality.co'].min())
    df['no2_norm'] = (df['current.air_quality.no2'] - df['current.air_quality.no2'].min()) / (
                df['current.air_quality.no2'].max() - df['current.air_quality.no2'].min())
    df['o3_norm'] = (df['current.air_quality.o3'] - df['current.air_quality.o3'].min()) / (
                df['current.air_quality.o3'].max() - df['current.air_quality.o3'].min())
    df['pm10_norm'] = (df['current.air_quality.pm10'] - df['current.air_quality.pm10'].min()) / (
                df['current.air_quality.pm10'].max() - df['current.air_quality.pm10'].min())
    df['pm25_norm'] = (df['current.air_quality.pm2_5'] - df['current.air_quality.pm2_5'].min()) / (
                df['current.air_quality.pm2_5'].max() - df['current.air_quality.pm2_5'].min())
    df['so2_norm'] = (df['current.air_quality.so2'] - df['current.air_quality.so2'].min()) / (
                df['current.air_quality.so2'].max() - df['current.air_quality.so2'].min())
    df['p1_norm'] = (df['p1'] - df['p1'].min()) / (df['p1'].max() - df['p1'].min())
    df['p2_norm'] = (df['p2'] - df['p2'].min()) / (df['p2'].max() - df['p2'].min())

    df['air_quality'] = df[['co_norm', 'no2_norm', 'o3_norm', 'pm10_norm', 'pm25_norm', 'so2_norm']].mean(axis=1)
    df = df.drop(['co_norm', 'no2_norm', 'o3_norm', 'pm10_norm', 'pm25_norm', 'so2_norm', 'p1_norm', 'p2_norm'], axis=1)

    dict_data = df.to_dict(orient="records")
    dict_data[0]["id"] = str(uuid.uuid4())
    df_transformed = pd.DataFrame(dict_data)
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
