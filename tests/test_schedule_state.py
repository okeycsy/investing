import os
import tempfile
import unittest
from pathlib import Path

import schedule_state


class ScheduleStateTest(unittest.TestCase):
    def setUp(self):
        self.old_scheduled = os.environ.get("SCHEDULED_RUN")
        self.old_key = os.environ.get("SCHEDULE_DISPATCH_KEY")

    def tearDown(self):
        if self.old_scheduled is None:
            os.environ.pop("SCHEDULED_RUN", None)
        else:
            os.environ["SCHEDULED_RUN"] = self.old_scheduled

        if self.old_key is None:
            os.environ.pop("SCHEDULE_DISPATCH_KEY", None)
        else:
            os.environ["SCHEDULE_DISPATCH_KEY"] = self.old_key

    def test_manual_run_never_skips(self):
        os.environ.pop("SCHEDULED_RUN", None)
        os.environ["SCHEDULE_DISPATCH_KEY"] = "close:2026-07-06"

        with tempfile.TemporaryDirectory() as tmp:
            self.assertFalse(schedule_state.should_skip(Path(tmp) / "schedule_state.json"))

    def test_scheduled_key_skips_after_marking_complete(self):
        os.environ["SCHEDULED_RUN"] = "true"
        os.environ["SCHEDULE_DISPATCH_KEY"] = "close:2026-07-06"

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "schedule_state.json"

            self.assertFalse(schedule_state.should_skip(path))
            schedule_state.mark_completed(path)
            self.assertTrue(schedule_state.should_skip(path))


if __name__ == "__main__":
    unittest.main()
