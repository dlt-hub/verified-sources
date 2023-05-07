from typing import Generator, List, TypedDict, Dict
import random

import pytest
import mimesis
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    DateTime,
    Boolean,
    Text,
    func,
    text,
    schema as sqla_schema,
    ForeignKey,
)

from dlt.common.utils import chunks, uniq_id
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.common.pendulum import pendulum, timedelta


class SQLAlchemySourceDB:
    def __init__(self, credentials: ConnectionStringCredentials) -> None:
        self.credentials = credentials
        self.database_url = credentials.to_native_representation()
        self.schema = "my_dlt_source" + uniq_id()
        self.engine = create_engine(self.database_url)
        self.metadata = MetaData(schema=self.schema)
        self.table_infos: Dict[str, TableInfo] = {}

    def create_schema(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(sqla_schema.CreateSchema(self.schema, if_not_exists=True))

    def drop_schema(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                sqla_schema.DropSchema(self.schema, cascade=True, if_exists=True)
            )

    def get_table(self, name: str) -> Table:
        return self.metadata.tables[f"{self.schema}.{name}"]

    def create_tables(self) -> None:
        Table(
            "app_user",
            self.metadata,
            Column("id", Integer(), primary_key=True, autoincrement=True),
            Column("email", Text(), nullable=False, unique=True),
            Column("display_name", Text(), nullable=False),
            Column(
                "created_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            ),
            Column(
                'updated_at',
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            )
        )
        Table(
            "chat_channel",
            self.metadata,
            Column("id", Integer(), primary_key=True, autoincrement=True),
            Column(
                "created_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            ),
            Column("name", Text(), nullable=False),
            Column("active", Boolean(), nullable=False, server_default=text("true")),
            Column(
                'updated_at',
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            )
        )
        Table(
            "chat_message",
            self.metadata,
            Column("id", Integer(), primary_key=True, autoincrement=True),
            Column(
                "created_at",
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            ),
            Column("content", Text(), nullable=False),
            Column(
                "user_id",
                Integer(),
                ForeignKey("app_user.id"),
                nullable=False,
                index=True,
            ),
            Column(
                "channel_id",
                Integer(),
                ForeignKey("chat_channel.id"),
                nullable=False,
                index=True,
            ),
            Column(
                'updated_at',
                DateTime(timezone=True),
                nullable=False,
                server_default=func.now(),
            )
        )
        Table('has_composite_key',
              self.metadata,
              Column('a', Integer(), primary_key=True),
              Column('b', Integer(), primary_key=True),
              Column('c', Integer(), primary_key=True))

        self.metadata.create_all(bind=self.engine)

    def _fake_users(self, n: int = 8594) -> List[int]:
        person = mimesis.Person()
        user_ids: List[int] = []
        table = self.metadata.tables[f"{self.schema}.app_user"]
        info = self.table_infos.setdefault('app_user', dict(row_count=0, ids=[], created_at=IncrementingDate()))
        dt = info['created_at']
        for chunk in chunks(range(n), 5000):

            rows = [
                dict(email=person.email(unique=True), display_name=person.name(), created_at=next(dt), updated_at=next(dt))
                for i in chunk
            ]
            with self.engine.begin() as conn:
                result = conn.execute(table.insert().values(rows).returning(table.c.id))  # type: ignore
                user_ids.extend(result.scalars())
        info['row_count'] += n
        info['ids'] += user_ids
        return user_ids

    def _fake_channels(self, n: int = 500) -> List[int]:
        _text = mimesis.Text()
        dev = mimesis.Development()
        table = self.metadata.tables[f"{self.schema}.chat_channel"]
        channel_ids: List[int] = []
        info = self.table_infos.setdefault('chat_channel', dict(row_count=0, ids=[], created_at=IncrementingDate()))
        dt = info['created_at']
        for chunk in chunks(range(n), 5000):
            rows = [
                dict(name=" ".join(_text.words()), active=dev.boolean(), created_at=next(dt), updated_at=next(dt)) for i in chunk
            ]
            with self.engine.begin() as conn:
                result = conn.execute(table.insert().values(rows).returning(table.c.id))  # type: ignore
                channel_ids.extend(result.scalars())
        info['row_count'] += n
        info['ids'] += channel_ids
        return channel_ids

    def fake_messages(self, n: int=9402) -> List[int]:
        user_ids = self.table_infos['app_user']['ids']
        channel_ids = self.table_infos['chat_channel']['ids']
        _text = mimesis.Text()
        choice = mimesis.Choice()
        table = self.metadata.tables[f"{self.schema}.chat_message"]
        message_ids: List[int] = []
        info = self.table_infos.setdefault('chat_message', dict(row_count=0, ids=[], created_at=IncrementingDate()))
        dt = info['created_at']
        for chunk in chunks(range(n), 5000):
            rows = [
                dict(
                    content=_text.random.choice(_text.extract(["questions"])),
                    user_id=choice(user_ids),
                    channel_id=choice(channel_ids),
                    created_at=next(dt),
                    updated_at=next(dt)
                )
                for i in chunk
            ]
            with self.engine.begin() as conn:
                result = conn.execute(table.insert().values(rows).returning(table.c.id))
                message_ids.extend(result.scalars())
        info['row_count'] += len(message_ids)
        info['ids'].extend(message_ids)
        return message_ids

    def _fake_chat_data(self, n: int = 9402) -> None:
        self._fake_users()
        self._fake_channels()
        self.fake_messages()

    def insert_data(self) -> None:
        self._fake_chat_data()


class IncrementingDate:
    def __init__(self, start_value: pendulum.DateTime = None) -> None:
        self.started = False
        self.start_value = start_value or pendulum.now()
        self.current_value = self.start_value

    def __next__(self) -> pendulum.DateTime:
        if not self.started:
            self.started = True
            return self.current_value
        self.current_value += timedelta(seconds=random.randrange(0, 120))
        return self.current_value


class TableInfo(TypedDict):
    row_count: int
    ids: List[int]
    created_at: IncrementingDate
