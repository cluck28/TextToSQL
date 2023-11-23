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
    select,
    Numeric,
    Boolean,
)


TYPE_MAPPING = {
    "integer": Integer,
    "string": String(16),
    "number": Numeric,
    "boolean": Boolean,
}


class Invoice(BaseModel):
    id: int
    account_id: str
    first_transction_at: datetime
    last_transation_at: datetime
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
    engine = create_engine("sqlite:///:memory:")
    metadata_obj = MetaData()
    # create table based on Schema
    column_params = schema_to_columns(schema)
    table = Table(
        table_name,
        metadata_obj,
        *column_params,
    )
    metadata_obj.create_all(engine)


def create_invoice(invoice: Invoice):
    pass


def create_invoices(invoices: List[Invoice]):
    for invoice in invoices:
        create_invoice(invoice)


if __name__ == "__main__":
    test_invoice = Invoice
    invoice_schema = Invoice.model_json_schema()
    columns = schema_to_columns(Invoice)
    create_table(Invoice, "invoices")
