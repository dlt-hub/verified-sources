from typing import Optional, Any, Dict

import psycopg2
from psycopg2.extras import LogicalReplicationConnection, ReplicationCursor
import pypgoutput
from pypgoutput.decoders import (
    Begin, Commit, Relation, Insert, Update, Delete, Truncate
)

from dlt.common import logger
from dlt.common.typing import TDataItem, TDataItems
from dlt.common.schema.typing import TColumnSchema
from dlt.common.data_types.typing import TDataType

from .exceptions import (
    ReplicationConnectionError,
    CreatePublicationError,
    CreatePublicationInsufficientPrivilegeError,
    AddTableToPublicationError,
    AddTableToPublicationInsufficientPrivilegeError,
    ReplicationSlotDoesNotExistError,
)


def pg_to_dlt_type_mapper(pg_type_oid: int) -> TDataType:
    """Maps postgres data type OID to dlt data type."""

    def psy_to_dlt_type_mapper(psycopg2_type) -> TDataType:
        """Maps psycopg2 data type to dlt data type."""

        if psycopg2_type == psycopg2._psycopg.BOOLEAN:
            return "bool"
        return "bigint"
    
    psy = psycopg2.extensions.string_types[pg_type_oid]
    return psy_to_dlt_type_mapper(psy)


def to_dlt_column_schema(col: pypgoutput.decoders.ColumnType) -> TColumnSchema:
    """Converts pypgoutput ColumnType to dlt column schema."""

    return {
        "name": col.name,
        "primary_key": bool(col.part_of_pkey),
        "data_type": pg_to_dlt_type_mapper(col.type_id),
        # "atttypmod": col.atttypmod
    }


def cast_pg_to_json(val: Optional[str], data_type: TDataType) -> Any:
    """Converts pgoutput's value string representation into JSON value."""

    if data_type == "bool":
        if val == "t":
            return True
        elif val == "f":
            return False
        return None
    return val


def rep_conn(
    database: str, user: str, password: str, host: str, port: str = 5432
) -> psycopg2._psycopg.connection:
    """Returns Postgres replication connection.
    
    Raises an error if the user does not have the LOGIN or REPLICATION attribute assigned.
    """

    try:
        return psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port,        
            connection_factory=LogicalReplicationConnection,
        )
    except psycopg2.OperationalError as e:
        raise ReplicationConnectionError(
            f'Make sure the user "{user}" has the LOGIN and REPLICATION attributes assigned.'
        ) from e
    except Exception as e:
        raise ReplicationConnectionError from e


def get_dsn_dict(conn: psycopg2._psycopg.connection) -> Dict[str, str]:
    """Returns connection DSN as dictionary."""

    return dict([pair.split("=") for pair in conn.dsn.split(" ")])


def create_publication(name: str, cur: ReplicationCursor) -> None:
    """Creates a publication for logical replication if it doesn't exist yet.
    
    Raises error if the user does not have the CREATE privilege for the database.
    """

    try:
        cur.execute(f"CREATE PUBLICATION {name};")
    except psycopg2.errors.DuplicateObject:  # the publication already exists
        pass
    except psycopg2.errors.InsufficientPrivilege as e:
        dsn = get_dsn_dict(cur.connection)
        raise CreatePublicationInsufficientPrivilegeError(dsn["user"], dsn["dbname"]) from e
    except Exception as e:
        raise CreatePublicationError from e
    

def add_table_to_publication(name: str, publication_name: str, cur: ReplicationCursor) -> None:
    """Adds a table to a publication for logical replication if the table is not a member yet.

    Raises error if the user is not owner of the table.
    """

    try:
        cur.execute(f"ALTER PUBLICATION {publication_name} ADD TABLE {name};")
    except psycopg2.errors.DuplicateObject:  # table is already member of publication
        pass         
    except psycopg2.errors.InsufficientPrivilege as e:
        raise AddTableToPublicationInsufficientPrivilegeError(
            name, publication_name, get_dsn_dict(cur.connection)["user"]
        ) from e
    except Exception as e:
        raise AddTableToPublicationError(name, publication_name) from e
    

def create_replication_slot(name: str, cur: ReplicationCursor, output_plugin: str = "pgoutput") -> None:
    """Creates a replication slot if it doesn't exist yet."""

    try:
        cur.create_replication_slot(name, output_plugin=output_plugin)
    except psycopg2.errors.DuplicateObject:  # the replication slot already exists
        pass


def get_max_lsn(slot_name: str, options: Dict[str, str], cur: ReplicationCursor) -> Optional[int]:
    """Returns maximum Log Sequence Number (LSN) in replication slot.

    Returns None if the replication slot is empty.
    
    Raises error if the replication slot does not exist.
    """

    options_str = ", ".join(
        f"'{x}'" for xs in list(map(list, options.items())) for x in xs  # comma-separated value string
    )
    try:
        cur.execute(
            # subtract '0/0' to convert pg_lsn type to int (https://stackoverflow.com/a/73738472)
            "SELECT MAX(lsn) - '0/0' AS max_lsn "
            f"FROM pg_logical_slot_peek_binary_changes('{slot_name}', NULL, NULL, {options_str});"
        )
        return cur.fetchone()[0]
    except psycopg2.errors.UndefinedObject as e:
        raise ReplicationSlotDoesNotExistError(slot_name) from e


def cdc_rows(
    conn: LogicalReplicationConnection,
    publication_name: str,
    slot_name: str,
    upto_lsn: int,
) -> TDataItems:


    cur = conn.cursor()

    


    # drop_slot = False
    # if drop_slot:
    #     try:
    #         cur.drop_replication_slot(slot_name)
    #     except psycopg2.ProgrammingError as e:
    #         print("e:", e)


    options = {'publication_names': publication_name, 'proto_version': '1'}
    # try:
    #     cur.start_replication(slot_name=slot_name, decode=False, options=options)
    # except psycopg2.ProgrammingError:
    #     cur.create_replication_slot(slot_name, output_plugin='pgoutput')
    #     cur.start_replication(slot_name=slot_name, decode=False, options=options)


    consumer = ReplicationConsumer(upto_lsn)

    try:
        if consumer.upto_lsn is not None:
            cur.consume_stream(consumer)
    except psycopg2.extras.StopReplication:
        print("StopReplication")
    finally:
        cur.close()
        yield consumer.data_items


class ReplicationConsumer(object):
    def __init__(self, upto_lsn):
        self.upto_lsn = upto_lsn
        self.relations = dict()
        self.data_items: TDataItems = []
        
        
    def __call__(self, msg: psycopg2.extras.ReplicationMessage):
        self.process_msg(msg)
        # msg.cursor.send_feedback(flush_lsn=msg.data_start, force=True)       
        if msg.data_start == self.upto_lsn:
            raise psycopg2.extras.StopReplication
        

    def process_msg(self, msg: psycopg2.extras.ReplicationMessage):
        op = (msg.payload[:1]).decode('utf-8')
        if op == 'B':
            print(Begin(msg.payload))
        elif op == "C":
            print(Commit(msg.payload))
        elif op == "R":
            self.process_relation(Relation(msg.payload))
        elif op == "I":
            self.process_insert(Insert(msg.payload))
        elif op == "U":
            print(Update(msg.payload))
        elif op == 'D':
            print(Delete(msg.payload))
        elif op == 'T':
            print(Truncate(msg.payload))
        else:
            pass


    def process_relation(self, decoded_msg):
        column_schema_list = [to_dlt_column_schema(c) for c in decoded_msg.columns]
        relation = {"name": decoded_msg.relation_name, "columns": column_schema_list}
        self.relations[decoded_msg.relation_id] = relation


    def process_insert(self, decoded_msg):
        column_schema_list = self.relations[decoded_msg.relation_id]["columns"]
        column_data_list = decoded_msg.new_tuple.column_data
        columns = zip(column_schema_list, column_data_list)

        data_item: TDataItem = {
            schema["name"]: cast_pg_to_json(
                data.col_data, schema["data_type"]
            ) for (schema, data) in columns
        }
        self.data_items.append(data_item)
