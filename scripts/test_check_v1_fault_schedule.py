import unittest

from scripts.check_v1_fault_schedule import parse_schedule


class FaultScheduleTests(unittest.TestCase):
    def test_normalizes_ordered_schedule_to_offsets(self):
        events = parse_schedule("leader@25,coordinator@50,combined@75", 200)

        self.assertEqual(
            [(event.mode, event.planned_percent, event.planned_offset_seconds) for event in events],
            [("leader", 25, 50), ("coordinator", 50, 100), ("combined", 75, 150)],
        )

    def test_rejects_unknown_mode(self):
        with self.assertRaisesRegex(ValueError, "unsupported"):
            parse_schedule("controller@25", 200)

    def test_rejects_non_increasing_or_final_percentage(self):
        for schedule in ("leader@25,coordinator@25", "leader@100"):
            with self.subTest(schedule=schedule):
                with self.assertRaisesRegex(ValueError, "increase|below"):
                    parse_schedule(schedule, 200)

    def test_rejects_duration_outside_campaign_bounds(self):
        for duration in (59, 21_601):
            with self.subTest(duration=duration):
                with self.assertRaisesRegex(ValueError, "between"):
                    parse_schedule("leader@25", duration)


if __name__ == "__main__":
    unittest.main()
