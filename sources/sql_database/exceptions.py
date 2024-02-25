class ReplicationConnectionError(Exception):
    def __init__(self, msg: str = "") -> None:
        msg = "Failed creating a connection for logical replication. " + msg
        super().__init__(msg)


class CreatePublicationError(Exception):
    def __init__(self, msg: str = "") -> None:
        msg = "Failed creating publication for logical replication. " + msg
        super().__init__(msg)


class CreatePublicationInsufficientPrivilegeError(CreatePublicationError):
    def __init__(self, user: str, database: str) -> None:
        self.user = user
        self.database = database
        super().__init__(
            f'Make sure the user "{user}" has the CREATE privilege for database "{database}".'
        )


class AddTableToPublicationError(Exception):
    def __init__(self, table_name:str, publication_name: str, msg: str = "") -> None:
        self.table_name = table_name
        self.publication_name = publication_name
        msg = f'Failed adding table "{table_name}" to publication "{publication_name}". ' + msg
        super().__init__(msg)


class AddTableToPublicationInsufficientPrivilegeError(AddTableToPublicationError):
    def __init__(self, table_name:str, publication_name: str, user: str) -> None:
        self.table_name = table_name
        self.publication_name = publication_name
        super().__init__(
            table_name,
            publication_name,
            f'Make sure the user "{user}" is owner of table "{table_name}".'
        )


class ReplicationSlotDoesNotExistError(Exception):
    def __init__(self, slot_name: str) -> None:
        self.slot_name = slot_name
        super().__init__(
            f'The replication slot "{slot_name}" does not exist on the Postgres instance.'
        )