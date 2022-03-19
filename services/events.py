import asyncio


class Periodic_Event():
    def __init__(self, loop_period=5):
        self.running = False
        self.event = asyncio.Event()
        self.loop_period = loop_period

    async def loop(self, n_events=None):
        if self.running:
            return
        else:
            self.running = True
            if n_events is None:
                while True:
                    await asyncio.sleep(self.loop_period)
                    self.event.set()
                    self.event.clear()
            else:
                for _ in range(n_events):
                    await asyncio.sleep(self.loop_period)
                    self.event.set()
                    self.event.clear()
                self.running = False


