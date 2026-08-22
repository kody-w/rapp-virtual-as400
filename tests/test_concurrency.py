from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from .support import EngineTestCase


class ConcurrencyTests(EngineTestCase):
    def test_concurrent_inserts_are_serialized_without_loss(self) -> None:
        self.bootstrap()

        def insert(index: int) -> None:
            self.engine.chat(
                "INSERT FILE(TEST/ITEMS) "
                f"VALUES(ID='I{index}',QTY='{index}',PRICE='{index}.00',NOTE='worker')",
                f"s-{index}",
                f"k-{index}",
            )

        with ThreadPoolExecutor(max_workers=12) as executor:
            list(executor.map(insert, range(40)))
        records = self.engine.store.snapshot()["libraries"]["TEST"]["files"]["ITEMS"]["records"]
        self.assertEqual(len(records), 40)
        self.assertEqual(len({record["ID"] for record in records}), 40)
