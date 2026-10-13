"""A finished replication batch is saved even when the slot is not fully drained."""

import pg_replication


class Batch:
    def __init__(self, lsn, done, item):
        self.last_commit_lsn = lsn
        self.generated_all = done
        self.item = item

    def __iter__(self):
        yield self.item


def test_a_finished_batch_is_saved_before_the_next_one_starts():
    specs = [(100, False, "batch-1"), (200, True, "batch-2")]
    state = {}

    def fake_generator(**kwargs):
        lsn, done, item = specs.pop(0)
        return Batch(lsn, done, item)

    original = (
        pg_replication.ItemGenerator,
        pg_replication.get_max_lsn,
        pg_replication.advance_slot,
        pg_replication.dlt.current.resource_state,
    )
    pg_replication.ItemGenerator = fake_generator
    pg_replication.get_max_lsn = lambda *args, **kwargs: 200
    pg_replication.advance_slot = lambda *args, **kwargs: None
    pg_replication.dlt.current.resource_state = lambda: state
    try:
        iterator = iter(
            pg_replication.replication_resource(
                "slot",
                "pub",
                credentials="postgresql://user:pass@localhost/db",
            )
        )
        assert next(iterator) == "batch-1"
        assert state == {}
        assert next(iterator) == "batch-2"
        assert state["last_commit_lsn"] == 100
        iterator.close()
        assert state["last_commit_lsn"] == 100
    finally:
        (
            pg_replication.ItemGenerator,
            pg_replication.get_max_lsn,
            pg_replication.advance_slot,
            pg_replication.dlt.current.resource_state,
        ) = original
