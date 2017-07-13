from tests.unit.helpers import UnitTestCase, unittest_run_loop


class DeltaTestCase(UnitTestCase):
    @unittest_run_loop
    async def test_invalid_json(self):
        async with self.client.post(
                "/update", data="invalid json",
                headers={"Content-Type": "application/json"}) as request:
            self.assertEqual(request.status, 400)

    @unittest_run_loop
    async def test_invalid_data(self):
        async with self.client.post("/update", json=[None]) as request:
            self.assertEqual(request.status, 400)

    @unittest_run_loop
    async def test_empty_data(self):
        async with self.client.post("/update",
                                    json={"delta": []}) as request:
            self.assertEqual(request.status, 204)
