# Examples

## Schemas

| File | What it makes |
|---|---|
| [customer.yaml](customer.yaml) | One flat record per customer. Copy it to start your own |
| [shop.yaml](shop.yaml) | Customers and their orders, as two related tables |

```bash
datafeed generate --schema examples/customer.yaml --count 10
```

## Reading a feed from your application

Each client below fetches two pages and then follows a stream, using only the language's standard library. First start `datafeed serve` in another terminal,
and create a feed:

```bash
datafeed feed create --schema examples/customer.yaml --seed 42 --name customers
```

Then run one, with the `fed_` ID it printed:

```bash
python examples/clients/read_feed.py <feed-id>
```

```bash
node examples/clients/read_feed.mjs <feed-id>
```

```bash
go run ./examples/clients/go <feed-id>
```

Each one also shows how to carry on after a dropped connection. If your server has authentication on, set `DATAFEED_TOKEN`, which they all send as a bearer token.
Set `DATAFEED_SERVER` if it isn't at `http://127.0.0.1:8080`.

## Following a session while it runs

A session writes its output as parts, and each part can be read as soon as it is committed. [live-session.yaml](live-session.yaml) closes a part every
second and runs one work unit at a time, so its records can be followed in order, about a second behind. Create and start it:

```bash
datafeed session create -f examples/live-session.yaml --start
```

Then follow it with the `ses_` ID it printed. This prints each record once, and stops when the session ends:

```bash
powershell -ExecutionPolicy Bypass -File examples/clients/follow_session.ps1 -Session <session-id>
```

The session runs until you stop it with `datafeed session stop <session-id>`. For records the moment they are generated, use a feed instead.
