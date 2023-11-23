from pydantic import BaseModel
from datetime import datetime
from typing import List
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, select


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


def create_table(schema: BaseModel, table_name: str):
    engine = create_engine("sqlite:///:memory:")
    metadata_obj = MetaData()
    # create table based on Schema
    city_stats_table = Table(
        table_name,
        metadata_obj,
        Column("city_name", String(16), primary_key=True),
        Column("population", Integer),
        Column("country", String(16), nullable=False),
    )
    metadata_obj.create_all(engine)


def create_invoice(invoice: Invoice):
    pass


def create_invoices(invoices: List[Invoice]):
    for invoice in invoices:
        create_invoice(invoice)


if __name__ == "__main__":
    test_invoice = Invoice
    print(test_invoice)
    print(Invoice.model_json_schema())
