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
    """Correctly sniff out if the code provided is sync or not and run in
    another thread if not.
    """
    if inspect.iscoroutine(callable):
        asyncio.create_task(callable)
    elif inspect.iscoroutinefunction(callable):
        asyncio.create_task(callable())
    else:
        asyncio.get_running_loop().run_in_executor(None, callable)


async def schedule(schedule: str, callable):
    """Very simple scheduler: set this, use `create_task`, and you can later
    .cancel() it if it needs to stop.
    """
    cron = crontab.CronTab(schedule)
    while True:
        await asyncio.sleep(cron.next(default_utc=True))
        run_code(callable)


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
                # Everythinfg runs in relation to when this code is first entered
                now = datetime.datetime.now(tz=datetime.UTC)
                # If the earliest scheduled job is set to run now or before...
                while self.schedule and self.schedule[0].scheduled_time <= now:
                    # Take it out of the schedule
                    cron_item = heapq.heappop(self.schedule)
                    # Run its associated code
                    run_code(cron_item.callable)

                    # Try to determine if it should be scheduled for another run
                    next_time = None
                    try:
                        # Ask for more values from iterator until we get one in the future
                        while next_time := next(cron_item.next_time_iterator) <= now:
                            pass
                    except StopIteration:
                        # Iterator is exhausted, assume task it to stay out of schedule
                        pass

                    # Is the task ready to be scheduled again?
                    if next_time is not None:
                        # Add to schedule heap
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

                # Determine how long to sleep until next scheduled task will be ready
                time_to_sleep = 1.0
                if self.schedule:
                    time_to_sleep = (
                        self.schedule[0].scheduled_time - now
                    ).total_seconds()
                    if time_to_sleep < 0:
                        time_to_sleep = 0

                # Wait for next scheduled task to be ready OR for the Event, which is a
                # trigger letting us know the schedule has changed and the sleep value
                # has potentially been invalidated
                _, pending = await asyncio.wait(
                    [
                        asyncio.create_task(asyncio.sleep(time_to_sleep)),
                        asyncio.create_task(self.event.wait()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                # Cancel the sleep or the event.wait, depending on which other was
                # the cause of the interruption
                for pending_item in pending:
                    pending_item.cancel()
            else:
                # Nothing in the schedule, sleep for a nominal amount of time
                # so we don't space heater the CPU
                await asyncio.sleep(1.0)
            self.event.clear()

    def add_to_schedule(self, cron_line, callable) -> uuid.UUID:
        # Create an iterator that respects the CronTab's intervals
        def next_fn(cron: crontab.CronTab):
            while True:
                yield datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(
                    seconds=cron.next(default_utc=True)
                )

        next_iter = next_fn(crontab.CronTab(cron_line))

        # Cronentry is a data strcuture that holds all the information needed
        # for each scheduled item.
        entry: CronEntry = CronEntry(
            # When it runs next
            scheduled_time=next(next_iter),
            # How to determine when it runs next after that
            next_time_iterator=next_iter,
            # What it calls
            callable=callable,
            # An ID for later cancellation
            id=uuid.uuid4(),
        )

        # Add item to schedule
        heapq.heappush(self.schedule, entry)
        # Trigger the `asyncio.wait` in the self._run coroutine to stop
        # via triggering the Event sync primitive
        self.event.set()

        # Return a generated Id, which can be used for .cancel() later
        return entry.id

    def cancel(self, id: uuid.UUID):
        # Remove item in schedule with target Id
        self.schedule = [item for item in self.schedule if item.id != id]
        # Re-heapify poisoned list
        heapq.heapify(self.schedule)
        # Trigger the `asyncio.wait` in the self._run coroutine to stop
        # via triggering the Event sync primitive
        self.event.set()


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
