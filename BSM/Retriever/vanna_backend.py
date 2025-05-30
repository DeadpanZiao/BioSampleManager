from vanna.chromadb import ChromaDB_VectorStore
import pandas as pd

from BSM.Retriever.open_ai_chat_customized import OpenAI_Chat

class BSMVanna(ChromaDB_VectorStore, OpenAI_Chat):
    def __init__(self, config=None):
        ChromaDB_VectorStore.__init__(self, config=config)

        OpenAI_Chat.__init__(self, config=config)

    def convert_sqlite_df_to_standard(self,df, table_name):
        """
        Converts a SQLite PRAGMA table_info DataFrame to a standard format compatible with INFORMATION_SCHEMA.COLUMNS.

        Args:
            df (pd.DataFrame): The input DataFrame from PRAGMA table_info.
            table_name (str): The name of the table.

        Returns:
            pd.DataFrame: A DataFrame with columns similar to INFORMATION_SCHEMA.COLUMNS.
        """
        # Map SQLite column names to standard column names
        mapping = {
            "cid": "ordinal_position",
            "name": "column_name",
            "type": "data_type",
            "notnull": "is_nullable",
            "dflt_value": "column_default",
            "pk": "primary_key"
        }

        # Rename columns based on the mapping
        df = df.rename(columns=mapping)

        # Add missing columns required by the original function
        df["table_catalog"] = "main"  # SQLite doesn't have a catalog concept; use "main"
        df["table_schema"] = "main"  # SQLite doesn't have schemas; use "main"
        df["table_name"] = table_name
        df["is_nullable"] = df["is_nullable"].apply(lambda x: "NO" if x else "YES")
        df["comment"] = None  # SQLite doesn't support comments on columns

        # Reorder columns to match the expected format
        standard_columns = [
            "table_catalog", "table_schema", "table_name",
            "column_name", "data_type", "is_nullable",
            "column_default", "ordinal_position", "comment"
        ]

        return df[standard_columns]

class BSMVannaWrapper:
    def __init__(self, api_key: str, db_path: str, model='gpt-4o', base_url='https://api.openai.com/v1/'):
        """
        Initialize the wrapper class for MyVanna.

        Args:
            api_key (str): The API key for OpenAI.
            db_path (str): The path to the SQLite database file.
        """
        # Initialize configuration
        self.config = {
            'api_key': api_key,
            'model': model,
            'base_url': base_url
        }

        # Initialize MyVanna instance
        self.vn = BSMVanna(config=self.config)
        self.vn.connect_to_sqlite(db_path)

    def train(self, table_name: str):
        """
        Train the model on the schema of a specific table using the training plan.

        Args:
            table_name (str): The name of the table to train on.
        """
        # Get the table information schema
        df_information_schema = self.vn.run_sql(f"PRAGMA table_info({table_name})")
        df_information_schema = self.vn.convert_sqlite_df_to_standard(df_information_schema, table_name)

        # Generate and execute the training plan
        plan = self.vn.get_training_plan_generic(df_information_schema)
        self.vn.train(plan=plan)

    def ask(self, question: str, table='Sample') -> tuple:
        """
        Ask a question and get the SQL query and result DataFrame.

        Args:
            question (str): The user's question.

        Returns:
            tuple: A tuple containing the SQL query (str) and the result DataFrame (pd.DataFrame).
        """
        self.train(table)
        sql, df, fig = self.vn.ask(question=question, visualize=False, allow_llm_to_see_data=True)
        return sql, df

# if __name__ == "__main__":
#     # Initialize the wrapper class with API parameters and database path
#     wrapper = BSMVannaWrapper(
#         api_key='sk-jxxxxxxxxxxxxxxxxxxxxxx',
#         db_path=r'path/to/your/xx.db'
#     )
#
#     # Train on a specific table
#     wrapper.train('Sample')
#
#     # Ask a question
#     sql, df = wrapper.ask(
#         question="What's the internal_id corresponding to GSE204684? "
#                  "The column geo_ids may contain more than one ID."
#     )
#
#     # Print results
#     print("Generated SQL Query:")
#     print(sql)
#     print("\nResult DataFrame:")
#     print(df)