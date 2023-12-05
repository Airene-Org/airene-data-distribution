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
def test_function(message: func.ServiceBusMessage, msgout: func.Out[str], doc: func.Out[func.Document]):
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

        # Set the reading to be saved
        reading = func.Document.from_dict(dict_data[0])
        doc.set(reading)  # Comment this out for dev purposes
        json_data = transformed_df.to_json(orient="records")
        msgout.set(json_data)  # Comment this out for dev purpose
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
    json_data = df.to_json(orient="records")
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
