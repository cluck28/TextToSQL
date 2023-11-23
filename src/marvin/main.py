from pydantic import BaseModel
from datetime import datetime
from typing import List
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Numeric,
    Boolean,
    insert,
    select,
    text,
)


TYPE_MAPPING = {
    "integer": Integer,
    "string": String(16),
    "number": Numeric,
    "boolean": Boolean,
}
ENGINE = create_engine("sqlite:///:memory:")


class Invoice(BaseModel):
    invoice_id: int
    account_id: str
    first_transaction_at: datetime
    last_transaction_at: datetime
    amount: float
    direct_success: bool
    has_failure: bool
    recovered: bool
    failed: bool


def schema_to_columns(schema: BaseModel) -> List[Column]:
    """
    Converts a pydantic schema into a list of Column
    objects for use in an ORM
    """
    columns = list()
    properties = schema.model_json_schema().get("properties")
    for key, value in properties.items():
        columns.append(Column(key, TYPE_MAPPING[value["type"]]))
    return columns


def create_table(schema: BaseModel, table_name: str):
    """
    Creates an ORM table based on a pydantic schema
    """
    metadata_obj = MetaData()
    # create table based on Schema
    column_params = schema_to_columns(schema)
    table = Table(
        table_name,
        metadata_obj,
        *column_params,
    )
    metadata_obj.create_all(ENGINE)
    return table


def create_row(table: Table, row: BaseModel):
    """
    Adds a pydantic entry to a sqlalchemy table
    """
    sql_row = row.model_dump()
    stmt = insert(table).values(**sql_row)
    with ENGINE.begin() as connection:
        cursor = connection.execute(stmt)


def create_rows(table: Table, rows: List[BaseModel]):
    for row in rows:
        create_row(table, row)


if __name__ == "__main__":
    # Create some test data
    test_invoice = Invoice(
        invoice_id=1,
        account_id="abc",
        first_transaction_at="2023-01-01T00:00:00Z",
        last_transaction_at="2023-01-08T00:00:00Z",
        amount=10.00,
        direct_success=True,
        has_failure=False,
        recovered=False,
        failed=False,
    )
    test_invoice1 = Invoice(
        invoice_id=2,
        account_id="abc",
        first_transaction_at="2023-01-02T00:00:00Z",
        last_transaction_at="2023-01-09T00:00:00Z",
        amount=15.00,
        direct_success=False,
        has_failure=True,
        recovered=True,
        failed=False,
    )
    # Set up the database
    columns = schema_to_columns(Invoice)
    table_name = "invoices"
    invoice_table = create_table(Invoice, table_name)
    test_invoices = [test_invoice, test_invoice1]
    create_rows(invoice_table, test_invoices)
    # Check that the database has been populated with data
    with ENGINE.connect() as con:
        rows = con.execute(text(f"SELECT * from {table_name}"))
        for row in rows:
            print(row)
