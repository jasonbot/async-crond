import asyncio
import contextlib
import dataclasses
import datetime
import heapq
import inspect
import typing
import uuid

import crontab


def run_code(callable):
    if inspect.iscoroutine(callable):
        asyncio.create_task(callable)
    elif inspect.iscoroutinefunction(callable):
        asyncio.create_task(callable())
    else:
        asyncio.get_running_loop().run_in_executor(None, callable)


async def schedule(schedule: str, function):
    cron = crontab.CronTab(schedule)
    while True:
        await asyncio.sleep(cron.next(default_utc=True))
        run_code(function)


@dataclasses.dataclass(frozen=True, order=True)
class CronEntry:
    scheduled_time: datetime.datetime
    id: uuid.UUID
    next_time_iterator: typing.Generator[datetime.datetime | None, None, None]
    callable: typing.Callable


class AsyncCron:
    schedule: list[CronEntry]
    event: asyncio.Event

    def __init__(self):
        self.schedule = []
        self.event = asyncio.Event()
        self.alive = True
        asyncio.create_task(self._run())

    async def _run(self):
        while self.alive:
            if self.schedule:
                now = datetime.datetime.now(tz=datetime.UTC)
                while self.schedule and self.schedule[0].scheduled_time <= now:
                    cron_item = heapq.heappop(self.schedule)
                    run_code(cron_item.callable)

                    next_time = None
                    try:
                        next_time = next(cron_item.next_time_iterator)
                    except StopIteration:
                        pass

                    if next_time is not None:
                        heapq.heappush(
                            self.schedule,
                            (
                                CronEntry(
                                    scheduled_time=next_time,
                                    next_time_iterator=cron_item.next_time_iterator,
                                    callable=cron_item.callable,
                                    id=cron_item.id,
                                )
                            ),
                        )

                time_to_sleep = 1.0
                if self.schedule:
                    time_to_sleep = (
                        self.schedule[0].scheduled_time - now
                    ).total_seconds()
                    if time_to_sleep < 0:
                        time_to_sleep = 0

                _, pending = await asyncio.wait(
                    [
                        asyncio.create_task(asyncio.sleep(time_to_sleep)),
                        asyncio.create_task(self.event.wait()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for pending_item in pending:
                    pending_item.cancel()
            else:
                await asyncio.sleep(1.0)
            self.event.clear()

    def add_to_schedule(self, cron_line, callable) -> uuid.UUID:
        def next_fn(cron: crontab.CronTab):
            while True:
                yield datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(
                    seconds=cron.next(default_utc=True)
                )

        next_iter = next_fn(crontab.CronTab(cron_line))

        entry: CronEntry = CronEntry(
            scheduled_time=next(next_iter),
            next_time_iterator=next_iter,
            callable=callable,
            id=uuid.uuid4(),
        )

        heapq.heappush(self.schedule, entry)
        self.event.clear()
        return entry.id

    def cancel(self, id: uuid.UUID):
        self.schedule = [item for item in self.schedule if item.id != id]
        heapq.heapify(self.schedule)
        self.event.clear()


@contextlib.contextmanager
def async_cron() -> typing.Iterator[AsyncCron]:
    cron = AsyncCron()
    yield cron
    cron.alive = False


if __name__ == "__main__":

    async def main():
        asyncio.create_task(
            schedule("*/1 * * * * * *", lambda: print("Simple task every 1 second"))
        )
        asyncio.create_task(
            schedule("*/3 * * * * * *", lambda: print("Simple task every 3 seconds"))
        )
        with async_cron() as cron:
            cron.add_to_schedule(
                "*/1 * * * * * *", lambda: print("Crond task every 1 second")
            )
            cancel_2_id = cron.add_to_schedule(
                "*/2 * * * * * *", lambda: print("Crond task every 2 seconds")
            )
            cron.add_to_schedule(
                "*/3 * * * * * *", lambda: print("Crond task every 3 seconds")
            )
            await asyncio.sleep(10.0)
            print("Cancelling the every 2 second call; should no longer see them")
            cron.cancel(cancel_2_id)
            cancel_2_id = cron.add_to_schedule(
                "*/2 * * * * * *", lambda: print("New crond task every 2 seconds")
            )
            await asyncio.sleep(20.0)

    asyncio.run(main())
