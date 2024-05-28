# coding: utf-8


"""
FILE: sample_query_table.py
DESCRIPTION:
    These samples demonstrate the following: querying a table for entities.
USAGE:
    python sample_query_table.py
    Set the environment variables with your own values before running the sample:
    1) AZURE_STORAGE_CONNECTION_STRING - the connection string to your storage account
"""
import os
import logging
import pandas as pd
from azure.data.tables import TableClient, TableEntity#, TableTransaction
from azure.core.exceptions import HttpResponseError
from assessment_episode_matcher.utils.environment import ConfigKeys
# import mylogging

# logging = mylogging.get(__name__)

class SampleTablesQuery(object):

    def __init__(self, table_name:str):
         
      self.connection_string =  str(os.environ.get(ConfigKeys.AZURE_STORAGE_CONNECTION_STRING.value,""))

      self.table_name = table_name
      logging.info(f"SampleTablesQuery initialised with connection_string: {self.connection_string}")

        
    def query_table(self, select_fields:list[str], filter_template:str, query_params:dict|None):    
        """
          parameters = {u"lower": 20211201, u"upper": 20220110}
          name_filter = u"AssessmentDate ge @lower and AssessmentDate lt @upper"
        """
        with TableClient.from_connection_string(self.connection_string, self.table_name) as table_client:
            try:
                # TODO: Fix query_params= {u"lower": 20211201, u"upper": 20220110}
                if query_params and filter_template:
                  queried_entities = table_client.query_entities(
                      query_filter=filter_template, select=select_fields, parameters=query_params
                  )
                else:
                  queried_entities = table_client.list_entities()
                # # Using query_filter with parameter
                # entities2 = table_client.query_entities(query_filter="Age gt @age", parameter={"age": 21})


                for entity_chosen in queried_entities:
                    timestamp = entity_chosen._metadata["timestamp"]
                    entity_chosen['Timestamp']=timestamp #.strftime("%Y-%m-%dT%H:%M:%SZ") # type: ignore
                    yield entity_chosen

            except HttpResponseError as e:
                print(e.message)
      


    def batch_insert_data(self, data: pd.DataFrame):
      with TableClient.from_connection_string(self.connection_string, self.table_name) as table_client:
        try:
          # Group DataFrame by 'PartitionKey'
          grouped = data.groupby('PartitionKey')

          for partition_key, group in grouped:
            group_dict = group.to_dict('records')
            # Convert keys to strings
            group_dict = [{str(k): v for k, v in entity.items()} for entity in group_dict]
            transaction_actions = [("create", TableEntity(**entity)) for entity in group_dict]

            logging.debug(f"Transaction actions: {transaction_actions}")

            response = table_client.submit_transaction(transaction_actions)
            logging.info(f"Batch data with PartitionKey {partition_key} successfully inserted into table {self.table_name}. Response: {response}")

        except HttpResponseError as e:
          logging.error(f"An error occurred: {e.message}")
        except Exception as e:
          logging.error(f"An unexpected error occurred: {e}")          



    # def batch_insert_data(self,  data: list[dict]):
    #     with TableClient.from_connection_string(self.connection_string, self.table_name) as table_client:
    #         try:
    #             transaction_actions = []
    #             for entity in data:
    #                 table_entity = TableEntity(**entity)
    #                 transaction_actions.append(("create", table_entity))

    #             response = table_client.submit_transaction(transaction_actions)
    #             logging.info(f"Batch data successfully inserted into table {self.table_name}. Response: {response}")

    #         except HttpResponseError as e:
    #             logging.error(f"An error occurred: {e.message}")


    # def batch_insert_data(self, data: list[dict]) -> int: #data: pd.DataFrame):
    #   with TableClient.from_connection_string(self.connection_string, self.table_name) as table_client:
    #       try:
    #           transaction = TableTransaction()
    #           for entity in data:                            
    #             table_entity = TableEntity(**entity)
    #             transaction.add_action("create", table_entity)

    #           response = table_client.submit_transaction(transaction)
    #           logging.info(f"Batch data successfully inserted into table {self.table_name}. Response: {response}")
    #           return 0

    #       except HttpResponseError as e:
    #           logging.error(f"An error occurred: {e.message}")
    #           return -1             

    # def insert_data(self, table_name: str, data: pd.DataFrame):
    #       with TableClient.from_connection_string(self.connection_string, table_name) as table_client:
    #           try:
    #               for _, row in data.iterrows():
    #                   entity = {column: row[column] for column in data.columns}
    #                   table_entity = TableEntity(**entity)
    #                   table_client.create_entity(entity=table_entity)
    #               logging.info(f"Data successfully inserted into table {table_name}")

    #           except HttpResponseError as e:
    #               logging.error(f"An error occurred: {e.message}")


# def build_query_components(filter):
#   name_filter = u"AssessmentDate ge @lower and AssessmentDate lt @upper"

# def main() -> list[dict]:
#     stq = SampleTablesQuery()

#     fields = [u"PartitionKey", u"RowKey", u"Progam",  u"SurveyName", u"SurveyData"]
#     assessment_date_limits = {u"lower": 20211201, u"upper": 20220110}
    
#     name_filter = u"AssessmentDate ge @lower and AssessmentDate lt @upper and IsActive eq 1 and Progam ne 'TEST' and Status eq 'Complete'"
#     results = [
#          get_json_result(json_atom) 
#          for json_atom in 
#          stq.query_atoms(fields, filter_template=name_filter, query_params=assessment_date_limits)
#          ]
#     return results


# if __name__ == "__main__":
#   results = main()
#   print(results)
    
        # stq.insert_random_entities()
        # stq.sample_query_entities()
        # stq.sample_query_entities_multiple_params()
        # stq.sample_query_entities_values()
    
        # stq.clean_up()


   # def sample_query_entities(self):
    #     from azure.data.tables import TableClient
    #     from azure.core.exceptions import HttpResponseError

    #     print("Entities with name: marker")
    #     # [START query_entities]
    #     with TableClient.from_connection_string(self.connection_string, self.table_name) as table_client:
    #         try:
    #             parameters = {"name": "marker"}
    #             name_filter = "Name eq @name"
    #             queried_entities = table_client.query_entities(
    #                 query_filter=name_filter, select=["Brand", "Color"], parameters=parameters
    #             )

    #             for entity_chosen in queried_entities:
    #                 print(entity_chosen)

    #         except HttpResponseError as e:
    #             print(e.message)
    #     # [END query_entities]

    # def sample_query_entities_multiple_params(self):
    #     from azure.data.tables import TableClient
    #     from azure.core.exceptions import HttpResponseError

    #     print("Entities with name: marker and brand: Crayola")
    #     # [START query_entities]
    #     with TableClient.from_connection_string(self.connection_string, self.table_name) as table_client:
    #         try:
    #             parameters = {"name": "marker", "brand": "Crayola"}
    #             name_filter = "Name eq @name and Brand eq @brand"
    #             queried_entities = table_client.query_entities(
    #                 query_filter=name_filter, select=["Brand", "Color"], parameters=parameters
    #             )

    #             for entity_chosen in queried_entities:
    #                 print(entity_chosen)

    #         except HttpResponseError as e:
    #             print(e.message)
    #     # [END query_entities]

    # def sample_query_entities_values(self):
    #     from azure.data.tables import TableClient
    #     from azure.core.exceptions import HttpResponseError

    #     print("Entities with 25 < Value < 50")
    #     # [START query_entities]
    #     with TableClient.from_connection_string(self.connection_string, self.table_name) as table_client:
    #         try:
    #             parameters = {"lower": 25, "upper": 50}
    #             name_filter = "Value gt @lower and Value lt @upper"
    #             queried_entities = table_client.query_entities(
    #                 query_filter=name_filter, select=["Value"], parameters=parameters
    #             )

    #             for entity_chosen in queried_entities:
    #                 print(entity_chosen)

    #         except HttpResponseError as e:
    #             print(e.message)