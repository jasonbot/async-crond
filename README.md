# async-crond

A very basic crontab runner for asyncio code. Uses [`crontab`](https://pypi.org/project/crontab/) under the hood to generate schedules and [`heapq`](https://docs.python.org/3/library/heapq.html) to reliably handle large numbers of cron lines in the `async_cron` context manager.

## Usage

You can use `schedule` as a very simple per-line scheduler:

```python
asyncio.create_task(
    crond.schedule("*/1 * * * * * *", lambda: print("Simple task every 1 second"))
)
```

Or the more scalable `async_cron` context manager, which allows for a large number of cron items in an efficient way and allows for items to be cancelled:

```python
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
    await asyncio.sleep(10.0)
```

`schedule` and `async_cron` can accept both `def` and `async def` functions as arguments to `callable`.


## Distribution

This is one file. Just copy the file. Why bother adding another dependency?
